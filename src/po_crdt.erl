-module(po_crdt).

-type po_log_crdt() :: term().
-type actor()       :: pid() | atom().
-type read_op()     :: rd | term().
-type update_op()   :: term().
-type timestamp()   :: vclock:vclock().

-callback(new(actor()) -> po_log_crdt()).
-callback(effect(update_op(), timestamp(), actor(), po_log_crdt()) -> po_log_crdt()).
-callback(eval(read_op(), po_log_crdt()) -> term()).
