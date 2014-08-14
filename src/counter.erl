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
