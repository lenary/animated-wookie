%% -------------------------------------------------------------------
%%
%% po_crdt: Behaviour for PO-Log Op-Based CRDTs
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

-module(po_crdt).

-type po_log_crdt() :: term().
-type read_op()     :: rd | term().
-type update_op()   :: term().
-type timestamp()   :: vclock:vclock().

-callback(new() -> po_log_crdt()).
-callback(effect(update_op(), timestamp(), po_log_crdt()) -> po_log_crdt()).
-callback(eval(read_op(), po_log_crdt()) -> term()).
-callback(stable(timestamp(), po_log_crdt()) -> po_log_crdt()).

