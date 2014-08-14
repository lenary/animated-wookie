-module(trcb).

-export([
         init/1,
         add_peer/2,
         rem_peer/2,
         cast/2,
         deliver/4,
         retry_outgoing/1,
         stable/1
        ]).

%% Some networks lose messages, we'll assume that they get lost this proportion
%% of the time, but 
-define(NETWORK_LOSSINESS, 0.9).

-record(trcb, {
          current     :: actor(),
          peer_clocks :: [{actor(), timestamp()}],
          muted = ordsets:new() :: [actor()],
          outgoing_q = [] :: outgoing_q(),
          incoming_q = [] :: incoming_q()
         }).

-type actor() :: pid() | atom().
-type message() :: term().
-type timestamp() :: vclock:vclock().
-type state() :: #trcb{}.

-type outgoing_q() :: [outgoing_entry()].
-type outgoing_entry() :: {message(), timestamp(), [actor()]}.
-type incoming_q() :: [incoming_entry()].
-type incoming_entry() :: {message(), timestamp(),  actor() }.

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

-spec init(actor()) -> state().
init(CurrentPeer) ->
    PeerClocks = orddict:store(CurrentPeer, vclock:fresh(), orddict:new()),
    #trcb{current=CurrentPeer, peer_clocks=PeerClocks}.

-spec add_peer(actor(), state()) -> {ok, state()}.
add_peer(Peer, State = #trcb{peer_clocks=Clocks,muted=Muted}) ->
    case orddict:find(Peer, Clocks) of
        {ok, _} -> Muted1 = ordsets:del_element(Peer,Muted),
                   Clocks1 = Clocks;
        error   -> Muted1 = Muted,
                   Clocks1 = orddict:store(Peer, vclock:fresh(), Clocks)
    end,
    {ok, State#trcb{peer_clocks=Clocks1,muted=Muted1}}.

%% This mutes the peer, pauses delivery of messages to or from it.
%% Re-adding the peer continues delivery of messages, no messages are lost,
%% just delayed until the partition is healed.
-spec rem_peer(actor(), state()) -> {ok, [{message(), timestamp()}], state()}.
rem_peer(Peer, State = #trcb{muted=Muted}) ->
    Muted1 = ordsets:add_element(Peer, Muted),
    {ok, State#trcb{muted=Muted1}}.

%% Broadcast a message to all peers (perhaps). At least adds it to the outgoing queue,
%% even if it doesn't send the message.
-spec cast(message(), state()) -> {ok, timestamp(), state()}.
cast(Message, State=#trcb{current=Self,
                          peer_clocks=Clocks,
                          outgoing_q=Outgoing
                         }) ->
    %% 1. Increment current Peer's Clock
    Clock = orddict:fetch(Self, Clocks),
    Clock1 = vclock:increment(Self, Clock),
    Clocks1 = orddict:store(Self, Clock1, Clocks),

    %% 2. Add to outgoing queue
    Peers = peers(State),
    Outgoing1 = [{Message, Clock1, Peers} | Outgoing],
    
    %% 3. Attempt some sends
    Outgoing2 = attempt_sends(Outgoing1, State#trcb{peer_clocks=Clocks1}),
    Outgoing3 = prune_outgoing(Outgoing2),
    lager:info("[~p:?] outgoing: ~p -> ~p", [self(), Outgoing1, Outgoing3]),
    
    {ok, Clock1, State#trcb{peer_clocks=Clocks1,outgoing_q=Outgoing3}}.

%% Called at interval by the server to send any outstanding messages.
-spec retry_outgoing(state()) -> {ok, state()}.
retry_outgoing(State = #trcb{outgoing_q=[]}) ->
    {ok, State};
retry_outgoing(State = #trcb{outgoing_q=Outgoing}) ->
    Outgoing1 = attempt_sends(Outgoing, State),
    Outgoing2 = prune_outgoing(Outgoing1),
    lager:info("[~p:?] outgoing: ~p -> ~p", [self(), Outgoing, Outgoing2]),
    
    {ok, State#trcb{outgoing_q=Outgoing2}}.


-spec deliver(message(), timestamp(), actor(), state()) ->
                     {ok, [{message(),timestamp()}], state()}.
deliver(Message, Clock, Origin, State=#trcb{incoming_q=Incoming}) ->
    %% FIXME
    Incoming1 = [{Message, Clock, Origin} | Incoming],
    State1 = State#trcb{incoming_q = Incoming1},
    
    %% wat
    {ok, Ready, State2} = find_ready(State1),
    lager:info("[~p:?] incoming: ~p -> ~p", [self(), Incoming1, State2#trcb.incoming_q]),
    lager:info("[~p:?] peer_clocks: ~p -> ~p", [self(), State1#trcb.peer_clocks, State2#trcb.peer_clocks]),

    {ok, as_messages(Ready), State2}.


%% In this version we don't actually care about stability
-spec stable(state()) -> {stable, timestamp(), state()} | unstable.
stable(_State) ->
    unstable.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec peers(state()) -> [actor()].
peers(#trcb{current=Self,peer_clocks=Clocks}) ->
    [Peer || Peer <- orddict:fetch_keys(Clocks),
             Peer /= Self].

-spec as_messages([incoming_entry()]) -> [{message(),timestamp()}].
as_messages(Incoming) ->
    [{Message,Ts} || {Message, Ts, _Origin} <- Incoming].

%% This does some simulation of a flaky network. It may reorder messages,
%% and delivers a subset of the outgoing queue. It won't deliver to any peer
%% in #trcb.muted.
-spec attempt_sends(outgoing_q(), state()) -> outgoing_q().
attempt_sends([], _State) ->
    [];
attempt_sends([{_Message, _Clock, []}|Rest], State) ->
    attempt_sends(Rest, State);
attempt_sends([{Message, Clock, Peers}|Rest], State) ->
    [attempt_peer_sends(Message, Clock, Peers, State)|attempt_sends(Rest,State)].

-spec attempt_peer_sends(message(), timestamp(), [actor()],state()) ->
                                {message(), timestamp(), [actor()]}.
attempt_peer_sends(Message, Clock, Peers, State) ->
    LeftPeers = lists:filter(fun(Peer) ->
                                     %% If it's muted, don't attempt a send,
                                     %% and include in leftover peers
                                     ordsets:is_element(Peer, State#trcb.muted) orelse
                                         %% If send fails, include in leftover peers
                                         not attempt_peer_send(State#trcb.current,
                                                               Message,
                                                               Clock,
                                                               Peer)
                             end, Peers),

    {Message, Clock, LeftPeers}.


%% Does the send, which fails at a proportion of ?NETWORK_LOSINESS
%% returns a boolean which reflects whether or not the send was successful.
-spec attempt_peer_send(actor(), message(), timestamp(), actor()) -> boolean().
attempt_peer_send(Origin, Message, Clock, Peer) ->
    R = random:uniform(),
    if R =< ?NETWORK_LOSSINESS -> false;
       true -> ok = po_log_node:effect(Peer, Message, Clock, Origin),
               true
    end.

%% Removes any entries in the outgoing_q that don't have any peers left to send to
-spec prune_outgoing(outgoing_q()) -> outgoing_q().
prune_outgoing([]) -> [];
prune_outgoing([{_Message, _Ts, []} | Rest]) -> 
    prune_outgoing(Rest);
prune_outgoing([Entry | Rest]) -> 
    [Entry | prune_outgoing(Rest)].


%% Finds any messages from the incoming queue that can be delivered, and delivers them
-spec find_ready(state()) -> {ok, [incoming_entry()], state()}.
find_ready(State=#trcb{incoming_q=Incoming, muted=Muted, peer_clocks=PeerClocks}) ->
    {NotReady, MaybeReady} = lists:partition(fun({_Message,_Ts,Origin}) ->
                                                ordsets:is_element(Origin,Muted)
                                              end, Incoming),

    MaybeReady1 = sort_by_ts(MaybeReady),
    
    {Ready, NotReady1, PeerClocks1} = find_ready(MaybeReady1, {[], NotReady, PeerClocks}),

    %% I really fucking hope this puts the ones with the smallest vclocks first
    %% as we apply in this order. Sigh.
    Ready1 = sort_by_ts(Ready),

    {ok, Ready1, State#trcb{incoming_q=NotReady1, peer_clocks=PeerClocks1}}.

%% Does a fold over the entries that may be ready.
%% Various things happen
%% An entry is considered ready if its ts immediately succeeds the previous ts delivered from that
%% peer. If this is the case, PeerClocks is immediately updated with that vclock, and the entry is
%% added to ready. If not, then PeerClocks isn't updated and 
find_ready([], FoldState) ->
    FoldState;
find_ready([{_Message,Ts,Origin} = Entry | Rest], {Ready, NotReady, PeerClocks}) ->
    %% Append to Ready, doesn't matter where you put it on NotReady
    OriginTs = orddict:fetch(Origin, PeerClocks),
    case vclock:immediately_succeeds(Ts,OriginTs) of
        true ->  PeerClocks1 = orddict:store(Origin, vclock:merge([Ts,OriginTs]), PeerClocks),
                 find_ready(Rest, {Ready ++ [Entry], NotReady,           PeerClocks1});
        false -> find_ready(Rest, {Ready,            [Entry | NotReady], PeerClocks})
    end.

%% This may completely break - vclock:descends I doubt is total
-spec sort_by_ts([incoming_entry()]) -> [incoming_entry()].
sort_by_ts(MaybeReady) ->
    lists:sort(fun({_,TsA,_}, {_,TsB,_}) ->
                       vclock:lte(TsA,TsB)
               end, MaybeReady).
