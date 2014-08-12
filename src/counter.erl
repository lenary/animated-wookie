-module(counter).

-behaviour(op_crdt).

-export([new/2, prepare/3, effect/3, eval/2]).

new(_Arg, _Actor) ->
    0.

prepare(increment, _Actor, _N) ->
    increment;
prepare(decrement, _Actor, _N) ->
    decrement.

effect(increment, _Actor, N) ->
    N + 1;
effect(decrement, _Actor, N) ->
    N - 1.

eval(rd, N) ->
    N.

