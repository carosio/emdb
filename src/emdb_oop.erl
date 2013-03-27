%%-------------------------------------------------------------------
%% This file is part of EMDB - Erlang MDB API
%%
%% Copyright (c) 2012 by Aleph Archives. All rights reserved.
%%
%%-------------------------------------------------------------------
%% Redistribution and use in source and binary forms, with or without
%% modification, are permitted only as authorized by the OpenLDAP
%% Public License.
%%
%% A copy of this license is available in the file LICENSE in the
%% top-level directory of the distribution or, alternatively, at
%% <http://www.OpenLDAP.org/license.html>.
%%
%% Permission to use, copy, modify, and distribute this software for any
%% purpose with or without fee is hereby granted, provided that the above
%% copyright notice and this permission notice appear in all copies.
%%
%% THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
%% WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
%% MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
%% ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
%% WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
%% ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
%% OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
%%-------------------------------------------------------------------

-module(emdb_oop, [Handle]).


%%====================================================================
%% EXPORTS
%%====================================================================
-export([
         close/0,

	 txn_begin/0,
	 txn_begin_ro/0,
	 txn_commit/0,
	 txn_abort/0,

         put/2,
         append/2,
         get/1,
         del/1,

         update/2,

	 cursor_open/0,
	 cursor_close/0,
	 cursor_next/0,
	 cursor_set/1,
	 cursor_del/0,

         drop/0
        ]).


%%====================================================================
%% PUBLIC API
%%====================================================================

%%--------------------------------------------------------------------
%% closes the handle (removes dbi from env and releases env)  
%% 
%%--------------------------------------------------------------------
close() ->
    emdb_drv:close(Handle).

%%--------------------------------------------------------------------
%% stores a key-value-pair in the db associated with the handle.
%% Keys and values are erlang terms, if no transaction is running
%% on the handle, one will be started and commited after putting.
%%--------------------------------------------------------------------
put(Key, Val) ->
    emdb_drv:put(Handle, term_to_binary(Key), term_to_binary(Val)).


%%--------------------------------------------------------------------
%% Appends a key-value-pair to the database, needs to be called inside 
%% a transaction and the key must be the biggest (according to erlang-
%% term-order) in the whole db.
%%--------------------------------------------------------------------
append(Key, Val) ->
    emdb_drv:append(Handle, term_to_binary(Key), term_to_binary(Val)).

%%--------------------------------------------------------------------
%% Returns {ok, Value} for a given Key.
%% Same transaction-behaviour like put.
%%--------------------------------------------------------------------
get(Key)->
    case emdb_drv:get(Handle, term_to_binary(Key)) of
	{ok, V} ->
	    {ok, binary_to_term(V)};
	V ->
	    V
    end.

%%--------------------------------------------------------------------
%% Deletes a database-entry, same transaction-behaviour like put.
%%--------------------------------------------------------------------
del(Key) ->
    emdb_drv:del(Handle, term_to_binary(Key)).

%%--------------------------------------------------------------------
%% Updates a database entry, same transaction-behavour like put.
%%--------------------------------------------------------------------
update(Key, Val) ->
    emdb_drv:update(Handle, term_to_binary(Key), term_to_binary(Val)).

%%--------------------------------------------------------------------
%% Opens a cursor and associates it with the handle.
%% Will open a transaction if none is running.
%%--------------------------------------------------------------------
cursor_open() ->
    emdb_drv:cursor_open(Handle).

%%--------------------------------------------------------------------
%% Closes a cursor and commits the running transaction.
%%--------------------------------------------------------------------
cursor_close() ->
    emdb_drv:cursor_close(Handle).

%%--------------------------------------------------------------------
%% Returns the next database entry according to the db order (which
%% should be erlang-term-order).
%% Will only work on a handle that has an open cursor (and txn).
%%--------------------------------------------------------------------
cursor_next() ->
    case emdb_drv:cursor_next(Handle) of
	{ok, {K,V}} ->
	    {ok, {binary_to_term(K), binary_to_term(V)}};
	V ->
	    V
    end.

%%--------------------------------------------------------------------
%% Sets the cursor to Key and returns the according db entry.
%% Will only work on a handle that has an open cursor (and txn).
%%--------------------------------------------------------------------
cursor_set(Key) ->
    case emdb_drv:cursor_set(Handle, term_to_binary(Key)) of
	{ok, {K,V}} ->
	    {ok, {binary_to_term(K), binary_to_term(V)}};
	V ->
	    V
    end.

%%--------------------------------------------------------------------
%% Deletes the db entry at current cursor position, moves to the next
%% db entry afterwards.
%% Will only work on a handle that has an open cursor (and txn).
%%--------------------------------------------------------------------
cursor_del() ->
    case emdb_drv:cursor_del(Handle) of
	{ok, {K,V}} ->
	    {ok, {binary_to_term(K), binary_to_term(V)}};
	V ->
	    V
    end.

%%--------------------------------------------------------------------
%% Starts a transaction (txn) and associates it with the handle.
%%--------------------------------------------------------------------
txn_begin() ->
    emdb_drv:txn_begin(Handle).

%%--------------------------------------------------------------------
%% Starts a read-only transaction and associates it with the handle.
%%--------------------------------------------------------------------
txn_begin_ro() ->
    emdb_drv:txn_begin_ro(Handle).

%%--------------------------------------------------------------------
%% Commits a transaction, will close and disassociate a running cursor
%% as well.
%%--------------------------------------------------------------------
txn_commit() ->
    emdb_drv:txn_commit(Handle).

txn_abort() ->
    emdb_drv:txn_abort(Handle).

%%--------------------------------------------------------------------
%% Drops (deletes) the whole db.
%% Will free all pages, not closing the handle or the dbi.
%%--------------------------------------------------------------------
drop() ->
    emdb_drv:drop(Handle).

%%====================================================================
%% INTERNAL FUNCTIONS
%%====================================================================
