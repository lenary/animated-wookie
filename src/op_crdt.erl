-module(op_crdt).

-type op_crdt()     :: term().
-type actor()       :: pid() | atom().
-type read_op()     :: term().
-type update_op()   :: term().
-type prepared_op() :: term().

-callback(new(actor()) -> op_crdt()).
-callback(prepare(update_op(), actor(), op_crdt()) -> prepared_op()).
-callback(effect(prepared_op(), actor(), op_crdt()) -> op_crdt()).
-callback(eval(read_op(), op_crdt()) -> term()).

