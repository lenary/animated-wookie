-module(mv_register).
-behaviour(po_crdt).

-export([new/1,
         effect/4,
         eval/2,
         test/0
        ]).

new(_Actor) ->
    ordsets:new().

effect({wr, Val}, Ts, _Actor, Reg) ->
    ordsets:add_element({Ts, {wr, Val}}, Reg).

eval(rd, Reg) ->
    [Val || {TsA, {wr, Val}} <- Reg,
            not lists:any(fun({TsB,_}) ->
                                  vclock:lt(TsA, TsB)
                          end, Reg)
    ].

%% This depends on the delay of 500ms of effect message transitiions.
test() ->
    {ok, _} = po_log_node:start(a, ?MODULE),
    {ok, _} = po_log_node:start(b, ?MODULE),
    {ok, _} = po_log_node:start(c, ?MODULE),

    ok = po_log_node:add_peer(a,b),
    ok = po_log_node:add_peer(a,c),
    ok = po_log_node:add_peer(b,c),

    ok = po_log_node:update(a,{wr,a1}),
    %% make sure update gets to everywhere before next updates
    timer:sleep(700),

    %% effectively concurrent.
    ok = po_log_node:update(b,{wr,b1}),
    ok = po_log_node:update(a,{wr,a2}),

    %% Wait for stability
    timer:sleep(1000),

    Val = [a2,b1],
    Val = po_log_node:eval(a, rd),
    Val = po_log_node:eval(b, rd),
    Val = po_log_node:eval(c, rd).
