%% -------------------------------------------------------------------
%%
%% po_log_node: Prototype PO-Log Actor.
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
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

-module(po_log_node).
-behaviour(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start/2,
         add_peer/2,
         rem_peer/2,
         eval/2,
         update/2,
         effect/4
        ]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start(Name, Type) ->
    gen_server:start({local, Name}, ?MODULE, [Type, Name], []).

add_peer(Node, Peer) ->
    ok = gen_server:call(Node, {add_peer, Peer}, infinity),
    ok = gen_server:call(Peer, {add_peer, Node}, infinity).

rem_peer(Node, Peer) ->
    ok = gen_server:call(Node, {rem_peer, Peer}, infinity),
    ok = gen_server:call(Peer, {rem_peer, Node}, infinity).
    
eval(Node, Operation) ->
    gen_server:call(Node, {eval, Operation}, infinity).

update(Node, Operation) ->
    gen_server:call(Node, {update, Operation}, infinity).

effect(Node, Operation, Ts, Origin) ->
    gen_server:cast(Node, {effect, Operation, Ts, Origin}).


%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

-record(po_log_node, {
          name :: atom(),
          mod :: module(),
          dt :: term(),
          trcb :: term()
         }).

init([Mod,Name]) ->
    TRCB = trcb:init(Name),
    
    %% To do retransmission
    timer:send_interval(100, retry_outgoing),
    timer:send_interval(100, retry_incoming),
    
    DT = Mod:new(),
    lager:info("[~p:~p:~s] new() = ~p", [self(), Name, Mod, DT]),

    {ok, #po_log_node{name=Name, mod=Mod, dt=DT, trcb=TRCB}}.


handle_call({add_peer, NewPeer}, _From, State=#po_log_node{trcb=TRCB}) ->
    {ok, TRCB1} = trcb:add_peer(NewPeer, TRCB),
    {reply, ok, State#po_log_node{trcb=TRCB1}};

handle_call({rem_peer, OldPeer}, _From, State=#po_log_node{trcb=TRCB}) ->
    {ok, TRCB1} = trcb:rem_peer(OldPeer, TRCB),
    {reply, ok, State#po_log_node{trcb=TRCB1}};

handle_call({eval, Operation}, _From, State = #po_log_node{mod=Mod,dt=DT}) ->
    %% A Read
    Res = Mod:eval(Operation, DT),
    lager:info("[~p:~p:~s] eval(~p, ~p) = ~p", [self(),State#po_log_node.name, Mod, Operation, DT, Res]),
    {reply, Res, State};

handle_call({update, Operation}, _From, State = #po_log_node{mod=Mod,dt=DT}) ->
    TRCB = State#po_log_node.trcb,
    {ok, Ts, TRCB1} = trcb:cast(Operation, TRCB),

    %% Local Effect
    DT1 = apply_effect(State#po_log_node.name,Mod,Operation,Ts,DT),

    %% Stability
    MaybeStable = trcb:stable(TRCB1),
    DT2 = apply_stable(State#po_log_node.name, Mod, DT1, MaybeStable),

    {reply, ok, State#po_log_node{dt=DT2,trcb=TRCB1}};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast({effect, Operation, Ts, Origin}, State=#po_log_node{mod=Mod,dt=DT,trcb=TRCB}) ->
    %% TRCB Chooses what messages to deliver upon recieving this new message
    {ok, Effects, TRCB1} = trcb:deliver(Operation, Ts, Origin, TRCB),
    case Effects of
        [] -> ok;
        [_|_] -> lager:debug("[~p:~p:~s] effects delivered by ts ~p -> ~p", [self(), State#po_log_node.name, Mod, Ts, Effects])
    end,

    %% Local Effects
    DT1 = apply_effects(State#po_log_node.name, Mod, Effects, DT),

    %% Stability
    case Effects of
        [] -> DT2 = DT1;
        _  -> MaybeStable = trcb:stable(TRCB1),
              DT2 = apply_stable(State#po_log_node.name, Mod, DT1, MaybeStable)
    end,

    {noreply, State#po_log_node{dt=DT2,trcb=TRCB1}};

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(retry_outgoing, State = #po_log_node{trcb=TCRB}) ->
    %% Retry Outstanding Messages (remember: lossy network)
    {ok, TRCB1} = trcb:retry_cast(TCRB),
    {noreply, State#po_log_node{trcb=TRCB1}};

handle_info(retry_incoming, State= #po_log_node{mod=Mod,dt=DT,trcb=TRCB}) ->
    {ok, Effects, TRCB1} = trcb:retry_deliver(TRCB),
    case Effects of
        [] -> ok;
        _  -> lager:debug("[~p:~p:~s] effects delivered -> ~p", [self(), State#po_log_node.name, Mod, Effects])
    end,

    %% Local Effects
    DT1 = apply_effects(State#po_log_node.name, Mod, Effects, DT),

    %% Stability
    case Effects of
        [] -> DT2 = DT1;
        _  -> MaybeStable = trcb:stable(TRCB1),
              DT2 = apply_stable(State#po_log_node.name, Mod, DT1, MaybeStable)
    end,

    {noreply, State#po_log_node{trcb=TRCB1, dt=DT2}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

apply_effects(_Name, _Mod, [], DT) ->
    DT;
apply_effects(Name, Mod, [{Operation,Ts}|Rest], DT) ->
    DT1 = apply_effect(Name, Mod,Operation,Ts,DT),
    apply_effects(Name, Mod, Rest, DT1).

apply_effect(Name, Mod, Operation, Ts, DT) ->
    DT1 = Mod:effect(Operation, Ts, DT),
    lager:info("[~p:~p:~s] effect(~p, ~p, ~p) = ~p", [self(), Name, Mod, Operation, Ts, DT, DT1]),
    DT1.


apply_stable(_Name, _Mod, DT, unstable) ->
    DT;
apply_stable(Name, Mod, DT, {stable, Ts}) ->
    DT1 = Mod:stable(Ts, DT),

    case DT1 =:= DT of
        false -> lager:info("[~p:~p:~s] stable(~p, ~p) = ~p", [self(), Name, Mod, Ts, DT, DT1]);
        _     -> ok
    end,

    DT1.
