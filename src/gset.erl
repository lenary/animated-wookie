%% -------------------------------------------------------------------
%%
%% gset: Prototype Op-based Grow-only Set.
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

-module(gset).

-behaviour(po_crdt).

-export([new/1,
         effect/4,
         eval/2,
         test/0
        ]).

new(_Actor) ->
    ordsets:new().

effect({add, E}, Ts, _Actor, Set) ->
    ordsets:add_element({Ts, E}, Set).

eval(rd, Set) ->
    [ E || {_,E} <- ordsets:to_list(Set)].

test() ->
    {ok,_} = po_log_node:start(a,?MODULE),
    po_log_node:update(a,{add,a}),
    [a] = po_log_node:eval(a,rd).
