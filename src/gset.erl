-module(gset).

-behaviour(op_crdt).

-export([new/2, prepare/3, effect/3, eval/2]).

new(_Arg, _Actor) ->
    ordsets:new().

prepare({add, E}, _Actor, _Set) ->
    {add, E}.

effect({add, E}, _Actor, Set) ->
    ordsets:add_element(E, Set).

eval(rd, Set) ->
    ordsets:to_list(Set).
