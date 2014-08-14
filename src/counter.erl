%% -------------------------------------------------------------------
%%
%% counter: Protoype Op-based PN-Counter
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

-module(counter).

-behaviour(po_crdt).

-export([new/1,
         effect/4,
         eval/2,
         test/0
        ]).

%% Unbelievable inefficient implementation
%% but straight out the paper and simple.
new(_Actor) ->
    ordsets:new().

effect(increment, Ts, _Actor, Set) ->
    ordsets:add_element({Ts, increment}, Set);
effect(decrement, Ts, _Actor, Set) ->
    ordsets:add_element({Ts,decrement}, Set).

eval(rd, Set) ->
    Inc = length([1  || {_,increment} <- Set]),
    Dec = length([-1 || {_,decrement} <- Set]),
    Inc - Dec.

test() ->
    {ok,_} = po_log_node:start(a, ?MODULE),
    po_log_node:update(a,increment),
    1 = po_log_node:eval(a,rd).
