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
