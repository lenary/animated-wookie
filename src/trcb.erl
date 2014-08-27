%% -------------------------------------------------------------------
%%
%% trcb: Tagged Reliable Causal Broadcast Library
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(trcb).

-export([
         init/1,
         add_peer/2,
         rem_peer/2,
         cast/2,
         deliver/4,
         retry_cast/1,
         retry_deliver/1,
         stable/1
        ]).

%% Some networks lose messages, we'll assume that they get lost this proportion
%% of the time, but 
-define(NETWORK_LOSSINESS, 0.2).

-record(trcb, {
          current     :: actor(),
          clock       = vclock:fresh() :: timestamp(),
          peer_clocks = orddict:new()  :: [{actor(), timestamp()}],
          muted       = ordsets:new()  :: [actor()],
          outgoing_q  = []             :: outgoing_q(),
          incoming_q  = []             :: incoming_q()
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
    #trcb{current=CurrentPeer}.

-spec add_peer(actor(), state()) -> {ok, state()}.
add_peer(Peer, #trcb{current = Peer}) ->
    error(badarg);
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
rem_peer(Peer, #trcb{current=Peer}) ->
    error(badarg);
rem_peer(Peer, State = #trcb{muted=Muted}) ->
    Muted1 = ordsets:add_element(Peer, Muted),
    {ok, State#trcb{muted=Muted1}}.

%% Broadcast a message to all peers (perhaps). At least adds it to the outgoing queue,
%% even if it doesn't send the message.
-spec cast(message(), state()) -> {ok, timestamp(), state()}.
cast(Message, State=#trcb{current=Self,
                          clock=Clock,
                          outgoing_q=Outgoing
                         }) ->
    %% 1. Increment current Peer's Clock
    Clock1 = vclock:increment(Self, Clock),

    %% 2. Add to outgoing queue
    Peers = peers(State),
    Outgoing1 = [{Message, Clock1, Peers} | Outgoing],
    
    %% 3. Attempt some sends
    Outgoing2 = attempt_sends(Outgoing1, State#trcb{clock=Clock1}),
    lager:debug("[~p:?:?] outgoing: ~p -> ~p", [self(), Outgoing1, Outgoing2]),
    
    {ok, Clock1, State#trcb{clock=Clock1,outgoing_q=Outgoing2}}.

%% Called at interval by the server to send any outstanding messages.
-spec retry_cast(state()) -> {ok, state()}.
retry_cast(State = #trcb{outgoing_q=[]}) ->
    {ok, State};
retry_cast(State = #trcb{outgoing_q=Outgoing}) ->
    Outgoing1 = attempt_sends(Outgoing, State),
    lager:info("[~p:?:?] outgoing: ~p -> ~p", [self(), Outgoing, Outgoing1]),
    
    {ok, State#trcb{outgoing_q=Outgoing1}}.


-spec deliver(message(), timestamp(), actor(), state()) ->
                     {ok, [{message(),timestamp()}], state()}.
deliver(Message, Clock, Origin, State=#trcb{incoming_q=Incoming}) ->
    %% 1. Add to the Incoming Queue
    Incoming1 = [{Message, Clock, Origin} | Incoming],
    State1 = State#trcb{incoming_q = Incoming1},
    
    %% 2. Check the Incoming Queue for ready messages
    {ok, Ready, State2} = find_ready(State1),
    lager:debug("[~p:?:?] incoming: ~p -> ~p {~p}", [self(), Incoming1, State2#trcb.incoming_q, State#trcb.peer_clocks]),

    %% 3. Update the current peer's timestamp with delivered message timestamps
    State3 = merge_ts([Ts || {_,Ts,_} <- Ready], State2),

    %% 4. Deliver messages
    {ok, as_messages(Ready), State3}.


-spec retry_deliver(state()) -> {ok, [{message(), timestamp()}], state()}.
retry_deliver(State=#trcb{incoming_q=[]}) ->
    {ok, [], State};
retry_deliver(State) ->
    %% 1. Check the Incoming Queue for ready messages
    {ok, Ready, State1} = find_ready(State),
    lager:debug("[~p:?:?] incoming: ~p -> ~p {~p}", [self(), State#trcb.incoming_q, State1#trcb.incoming_q, State#trcb.peer_clocks]),

    %% 2. Update the current peer's timesamp with delivered mesage timestamps
    State2 = merge_ts([Ts || {_,Ts,_} <- Ready], State1),

    %% 3. Deliver messages
    {ok, as_messages(Ready), State2}.
    

%% I believe that stability == glb of all recieved timestamps
-spec stable(state()) -> {stable, timestamp()} | unstable.
stable(#trcb{peer_clocks=PeerClocks, clock=Clock}) ->

    Clocks  = [C || {_P,C} <- PeerClocks],
    GLB = lists:foldl(fun vclock:glb/2, Clock, Clocks),
    
    {stable, GLB}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec peers(state()) -> [actor()].
peers(#trcb{peer_clocks=Clocks}) ->
    orddict:fetch_keys(Clocks).

-spec as_messages([incoming_entry()]) -> [{message(),timestamp()}].
as_messages(Incoming) ->
    [{Message,Ts} || {Message, Ts, _Origin} <- Incoming].

%% This does some simulation of a flaky network. It may reorder messages,
%% and delivers a subset of the outgoing queue. It won't deliver to any peer
%% in #trcb.muted.
-spec attempt_sends(outgoing_q(), state()) -> outgoing_q().
attempt_sends(Outgoing, State) ->
    [attempt_peer_sends(Message, Clock, Peers, State)
     || {Message, Clock, Peers} <- Outgoing,
        Peers /= []
    ].


-spec attempt_peer_sends(message(), timestamp(), [actor()],state()) ->
                                {message(), timestamp(), [actor()]}.
attempt_peer_sends(Message, Clock, Peers, State) ->
    {MutedPeers,ReadyPeers} = lists:partition(fun(Peer) ->
                                                      Peer == State#trcb.current orelse
                                                          ordsets:is_element(Peer, State#trcb.muted)
                                              end, Peers),

    {_SuccessPeers,FailPeers} = lists:partition(fun(Peer) ->
                                                       attempt_peer_send(State#trcb.current,
                                                                         Message,
                                                                         Clock,
                                                                         Peer)
                                               end, ReadyPeers),
    
    {Message, Clock, MutedPeers ++ FailPeers}.


%% Does the send, which fails at a proportion of ?NETWORK_LOSINESS
%% returns a boolean which reflects whether or not the send was successful.
-spec attempt_peer_send(actor(), message(), timestamp(), actor()) -> boolean().
attempt_peer_send(Origin, Message, Clock, Peer) ->
    R = random:uniform(),
    if R =< ?NETWORK_LOSSINESS -> false;
       true -> ok = po_log_node:effect(Peer, Message, Clock, Origin),
               true
    end.


%% Finds any messages from the incoming queue that can be delivered, and delivers them
-spec find_ready(state()) -> {ok, [incoming_entry()], state()}.
find_ready(State=#trcb{incoming_q=[]}) ->
    {ok, [], State};
find_ready(State=#trcb{incoming_q=Incoming, muted=Muted, peer_clocks=PeerClocks, clock=Clock}) ->
    {NotReady, MaybeReady} = lists:partition(fun({_Message,_Ts,Origin}) ->
                                                ordsets:is_element(Origin,Muted)
                                              end, Incoming),

        %% find_ready may be incorrect, but I'm not sure.
    {Ready, NotReady1, PeerClocks1, _} = find_ready(MaybeReady, {[], NotReady, PeerClocks, Clock}),

    %% I really fucking hope this puts the ones with the smallest vclocks first
    %% as we apply in this order. Sigh.
    Ready1 = sort_by_ts(Ready),
    case Ready of
        [] -> ok;
        [_|_] -> lager:debug("sort_by_ts(~p) -> ~p", [Ready, Ready1])
    end,

    {ok, Ready1, State#trcb{incoming_q=NotReady1, peer_clocks=PeerClocks1}}.

%% Does a fold over the entries that may be ready.
%% Various things happen
%% An entry is considered ready if its ts immediately succeeds the previous ts delivered from that
%% peer. If this is the case, PeerClocks is immediately updated with that vclock, and the entry is
%% added to ready. If not, then PeerClocks isn't updated and 
find_ready([], FoldState) ->
    FoldState;
find_ready([{_Message,Ts,Origin} = Entry | Rest], {Ready, NotReady, PeerClocks, CurrentTs}) ->
    %% Append to Ready, doesn't matter where you put it on NotReady
    OriginTs = orddict:fetch(Origin, PeerClocks),
    case vclock:immediately_succeeds(Ts,vclock:merge([CurrentTs,OriginTs])) of
        true ->  PeerClocks1 = orddict:store(Origin, vclock:merge([Ts,OriginTs]), PeerClocks),
                 find_ready(Rest, {Ready ++ [Entry], NotReady,           PeerClocks1, CurrentTs});
        false -> find_ready(Rest, {Ready,            [Entry | NotReady], PeerClocks,  CurrentTs})
    end.

%% This may completely break - vclock:descends I doubt is total
-spec sort_by_ts([incoming_entry()]) -> [incoming_entry()].
sort_by_ts(MaybeReady) ->
    lists:sort(fun({_,TsA,_}, {_,TsB,_}) ->
                       vclock:lte(TsA,TsB)
               end, MaybeReady).

-spec merge_ts([timestamp()], state()) -> state().
merge_ts(Tss, State=#trcb{clock=Clock}) ->
    State#trcb{clock=vclock:merge([Clock|Tss])}.
