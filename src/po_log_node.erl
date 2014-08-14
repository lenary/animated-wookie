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
    ok = gen_server:call(Node, {add_peer, Peer}),
    ok = gen_server:call(Peer, {add_peer, Node}).

rem_peer(Node, Peer) ->
    ok = gen_server:call(Node, {rem_peer, Peer}).
    
eval(Node, Operation) ->
    gen_server:call(Node, {eval, Operation}).

update(Node, Operation) ->
    gen_server:call(Node, {update, Operation}).

effect(Node, Operation, Ts, Origin) ->
    gen_server:call(Node, {effect, Operation, Ts, Origin}).


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
    timer:send_interval(10, retry),
    
    DT = Mod:new(Name),
    lager:info("[~p:~s] new(self()) = ~p", [Name, Mod, DT]),

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
    lager:info("[~p:~s] eval(~p, ~p) = ~p", [State#po_log_node.name, Mod, Operation, DT, Res]),
    {reply, Res, State};

handle_call({update, Operation}, _From, State = #po_log_node{mod=Mod,dt=DT}) ->
    TRCB = State#po_log_node.trcb,
    {ok, Ts, TRCB1} = trcb:cast(Operation, TRCB),

    %% Local Effect
    DT1 = apply_effect(State#po_log_node.name,Mod,Operation,Ts,DT),

    {reply, ok, State#po_log_node{dt=DT1,trcb=TRCB1}};

handle_call({effect, Operation, Ts, Origin}, _From, State=#po_log_node{mod=Mod,dt=DT,trcb=TRCB}) ->
    %% TRCB Chooses what messages to deliver upon recieving this new message
    {ok, Effects, TRCB1} = trcb:deliver(Operation, Ts, Origin, TRCB),

    %% Local Effects
    DT1 = apply_effects(State#po_log_node.name, Mod, Effects, DT),
    
    {reply, ok, State#po_log_node{dt=DT1,trcb=TRCB1}};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(retry, State = #po_log_node{trcb=TCRB}) ->
    %% Retry Outstanding Messages (remember: lossy network)
    {ok, TRCB1} = trcb:retry_outgoing(TCRB),
    {noreply, State#po_log_node{trcb=TRCB1}};

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
    DT1 = Mod:effect(Operation, Ts, Name, DT),
    lager:info("[~p:~s] effect(~p, ~p, self(), ~p) = ~p", [Name, Mod, Operation, Ts, DT, DT1]),
    DT1.
