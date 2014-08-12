-module(po_log_node).
-behaviour(gen_server).

-compile([{parse_transform, lager_transform}]).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start/2,
         eval/2,
         update/3]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start(Type, Arg) ->
    gen_server:start_link(?MODULE, [Type, Arg], []).

eval(Node, Operation) ->
    gen_server:call(Node, {eval, Operation}).

update(Node, Operation, OtherNodes) ->
    gen_server:call(Node, {update, Operation, OtherNodes}).

effect(Node, PreparedOp) ->
    %% FIXME: something about transportation, somehow
    gen_server:cast(Node, {effect, PreparedOp}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

-record(po_log_node, {
          mod :: module(),
          dt :: term()
         }).

init([Mod, Arg]) ->
    DT = Mod:new(Arg, self()),
    lager:info("[~p:~s] new(~p, self()) = ~p", [self(), Mod, Arg, DT]),
    {ok, #po_log_node{mod=Mod, dt=DT}}.

handle_call({eval, Operation}, _From, State = #po_log_node{mod=Mod,dt=DT}) ->
    %% A Read
    Res = Mod:eval(Operation, DT),
    lager:info("[~p:~s] eval(~p, ~p) = ~p", [self(), Mod, Operation, DT, Res]),

    {reply, Res, State};
handle_call({update, Operation, OtherNodes}, _From, State = #po_log_node{mod=Mod,dt=DT}) ->
    %% Make Prepared Op
    POp = Mod:prepare(Operation, self(), DT),
    lager:info("[~p:~s] prepare(~p, self(), ~p) = ~p", [self(), Mod, Operation, DT, POp]),

    %% Send to remote for effect
    [effect(OtherNode, POp) || OtherNode <- OtherNodes, OtherNode /= self() ],

    %% Local effect
    DT1 = Mod:effect(POp, self(), DT),
    lager:info("[~p:~s] effect(~p, self(), ~p) = ~p", [self(), Mod, POp, DT, DT1]),

    {reply, ok, State#po_log_node{dt=DT1}};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({effect, PreparedOp}, State = #po_log_node{mod=Mod,dt=DT}) ->
    %% Local Effect of Remotely Prepared Op
    DT1 = Mod:effect(PreparedOp, self(), DT),
    lager:info("[~p:~s] effect(~p, self(), ~p) = ~p", [self(), Mod, PreparedOp, DT, DT1]),

    {noreply, State#po_log_node{dt=DT1}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

