%% -------------------------------------------------------------------
%%
%% mv_register: Prototype Op-based Multi-Value Register
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

-module(mv_register).
-behaviour(po_crdt).

-export([new/0,
         effect/3,
         eval/2,
         stable/2,
         test/0
        ]).

new() ->
    ordsets:new().

effect({wr, Val}, Ts, Reg) ->
    ordsets:add_element({Ts, {wr, Val}}, Reg).

eval(rd, Reg) ->
    [Val || {TsA, {wr, Val}} <- Reg,
            not lists:any(fun({TsB,_}) ->
                                  vclock:lt(TsA, TsB)
                          end, Reg)
    ].

stable(_Ts, Reg) ->
    Reg.

%% This depends on the delay of 500ms of effect message transitiions.
test() ->
    {ok, A} = po_log_node:start(a, ?MODULE),
    {ok, B} = po_log_node:start(b, ?MODULE),
    {ok, C} = po_log_node:start(c, ?MODULE),

    ok = po_log_node:add_peer(a,b),
    ok = po_log_node:add_peer(a,c),
    ok = po_log_node:add_peer(b,c),

    ok = po_log_node:update(a,{wr,a1}),

    ValTrue1 = [a1],
    ValA1 = po_log_node:eval(a,rd),
    ValB1 = po_log_node:eval(b,rd),
    ValC1 = po_log_node:eval(c,rd),

    ValTrue1 = ValA1 = ValB1 = ValC1,

    timer:sleep(200),
     
    %% Partition
    ok = po_log_node:rem_peer(a,b),
    ok = po_log_node:rem_peer(c,b),

    timer:sleep(100),
    
    %% Concurrent.
    ok = po_log_node:update(a,{wr,a2}),
    ok = po_log_node:update(b,{wr,b1}),
    [a2] = po_log_node:eval(a,rd),
    [b1] = po_log_node:eval(b,rd),

    timer:sleep(100),
    
    %% Heal Paritition
    ok = po_log_node:add_peer(a,b),
    ok = po_log_node:add_peer(c,b),

    timer:sleep(100),
    
    ValTrue = [a2,b1],
    ValA = po_log_node:eval(a, rd),
    ValB = po_log_node:eval(b, rd),
    ValC = po_log_node:eval(c, rd),

    ValTrue = ValA = ValB = ValC,

    exit(A,kill),
    exit(B,kill),
    exit(C,kill).
