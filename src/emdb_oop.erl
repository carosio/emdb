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

-define(EMDB_TIMEOUT, 10000).

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

         drop/0,
	 dummy_action/0,
	 bulk_txn/1,
	 bulk_txn_prepared/1,
	 prepare_bulk/1
        ]).


%%====================================================================
%% PUBLIC API
%%====================================================================

%%--------------------------------------------------------------------
%% closes the handle (removes dbi from env and releases env)  
%% 
%%--------------------------------------------------------------------
close() ->
    Ref = make_ref(),
    case emdb_drv:close(Ref, Handle) of
	ok ->
	    receive_answer(Ref, ?EMDB_TIMEOUT);
	V ->
	    V
    end.

%%--------------------------------------------------------------------
%% stores a key-value-pair in the db associated with the handle.
%% Keys and values are erlang terms, if no transaction is running
%% on the handle, one will be started and commited after putting.
%%--------------------------------------------------------------------
put(Key, Val) ->
    Ref = make_ref(),
    case emdb_drv:put(Ref, Handle, term_to_binary(Key), term_to_binary(Val)) of
	ok ->
	    receive_answer(Ref, ?EMDB_TIMEOUT);
	V ->
	    V
    end.

%%--------------------------------------------------------------------
%% Appends a key-value-pair to the database, needs to be called inside 
%% a transaction and the key must be the biggest (according to erlang-
%% term-order) in the whole db.
%%--------------------------------------------------------------------
append(Key, Val) ->
    Ref = make_ref(),
    case emdb_drv:append(Ref, Handle, term_to_binary(Key), term_to_binary(Val)) of
	ok ->
	    receive_answer(Ref, ?EMDB_TIMEOUT);
	V ->
	    V
    end.


%%--------------------------------------------------------------------
%% Returns {ok, Value} for a given Key.
%% Same transaction-behaviour like put.
%%--------------------------------------------------------------------
get(Key)->
    Ref = make_ref(),
    case emdb_drv:get(Ref, Handle, term_to_binary(Key)) of
	ok ->
	    case receive_answer(Ref, ?EMDB_TIMEOUT) of
		{ok, V} ->
		    {ok, binary_to_term(V)};
		V ->
		    V
	    end;
	V ->
	    V   
    end.

%%--------------------------------------------------------------------
%% Deletes a database-entry, same transaction-behaviour like put.
%%--------------------------------------------------------------------
del(Key) ->
    Ref = make_ref(),
    case emdb_drv:del(Ref, Handle, term_to_binary(Key)) of
	ok ->
	    receive_answer(Ref, ?EMDB_TIMEOUT);
	V ->
	    V
    end.

%%--------------------------------------------------------------------
%% Updates a database entry, same transaction-behavour like put.
%%--------------------------------------------------------------------
update(Key, Val) ->
    Ref = make_ref(),
    case emdb_drv:update(Ref, Handle, term_to_binary(Key), term_to_binary(Val)) of
	ok ->
	    receive_answer(Ref, ?EMDB_TIMEOUT);
	V ->
	    V
    end.

%%--------------------------------------------------------------------
%% Opens a cursor and associates it with the handle.
%% Will open a transaction if none is running.
%%--------------------------------------------------------------------
cursor_open() ->
    Ref = make_ref(),
    case emdb_drv:cursor_open(Ref,  Handle) of
	ok ->
	    receive_answer(Ref, ?EMDB_TIMEOUT);
	V ->
	    V
    end.

%%--------------------------------------------------------------------
%% Closes a cursor and commits the running transaction.
%%--------------------------------------------------------------------
cursor_close() ->
    Ref = make_ref(),
    case emdb_drv:cursor_close(Ref, Handle) of
	ok ->
	    receive_answer(Ref, ?EMDB_TIMEOUT);
	V ->
	    V
    end.

%%--------------------------------------------------------------------
%% Returns the next database entry according to the db order (which
%% should be erlang-term-order).
%% Will only work on a handle that has an open cursor (and txn).
%%--------------------------------------------------------------------
cursor_next() ->
    Ref = make_ref(),
    case emdb_drv:cursor_next(Ref, Handle) of
	ok ->
	    case receive_answer(Ref, ?EMDB_TIMEOUT) of
		{ok, {K,V}} ->
		    {ok, {binary_to_term(K), binary_to_term(V)}};
		V ->
		    V
	    end;
	V ->
	    V
    end.

%%--------------------------------------------------------------------
%% Sets the cursor to Key and returns the according db entry.
%% Will only work on a handle that has an open cursor (and txn).
%%--------------------------------------------------------------------
cursor_set(Key) ->
    Ref = make_ref(),
    case emdb_drv:cursor_set(Ref, Handle, term_to_binary(Key)) of
	ok ->
	    case receive_answer(Ref, ?EMDB_TIMEOUT) of
		{ok, {K,V}} ->
		    {ok, {binary_to_term(K), binary_to_term(V)}};
		V ->
		    V
	    end;
	V ->
	    V
    end.

%%--------------------------------------------------------------------
%% Deletes the db entry at current cursor position, moves to the next
%% db entry afterwards.
%% Will only work on a handle that has an open cursor (and txn).
%%--------------------------------------------------------------------
cursor_del() ->
    Ref = make_ref(),
    case emdb_drv:cursor_del(Ref, Handle) of
	ok ->
	    case receive_answer(Ref, ?EMDB_TIMEOUT) of
		{ok, {K,V}} ->
		    {ok, {binary_to_term(K), binary_to_term(V)}};
		V ->
		    V
	    end;
	V ->
	    V
    end.

%%--------------------------------------------------------------------
%% Starts a transaction (txn) and associates it with the handle.
%%--------------------------------------------------------------------
txn_begin() ->
    Ref = make_ref(),
    case emdb_drv:txn_begin(Ref,  Handle) of
	ok ->
	    receive_answer(Ref, ?EMDB_TIMEOUT);
	V ->
	    V
    end.
%%--------------------------------------------------------------------
%% Starts a read-only transaction and associates it with the handle.
%%--------------------------------------------------------------------
txn_begin_ro() ->
    Ref = make_ref(),
    case emdb_drv:txn_begin_ro(Ref, Handle) of
	ok ->
	    receive_answer(Ref, ?EMDB_TIMEOUT);
	V ->
	    V
    end.

%%--------------------------------------------------------------------
%% Commits a transaction, will close and disassociate a running cursor
%% as well.
%%--------------------------------------------------------------------
txn_commit() ->
    Ref = make_ref(),
    case emdb_drv:txn_commit(Ref, Handle) of
	ok ->
	    receive_answer(Ref, ?EMDB_TIMEOUT);
	V ->
	    V
    end.

txn_abort() ->
    Ref = make_ref(),
    case emdb_drv:txn_abort(Ref, Handle) of
	ok ->
	    receive_answer(Ref, ?EMDB_TIMEOUT);
	V ->
	    V
    end.

%%--------------------------------------------------------------------
%% Drops (deletes) the whole db.
%% Will free all pages, not closing the handle or the dbi.
%%--------------------------------------------------------------------
drop() ->
    Ref = make_ref(),
    case emdb_drv:drop(Ref, Handle) of
	ok ->
	    receive_answer(Ref, ?EMDB_TIMEOUT);
	V ->
	    V
    end.


dummy_action() ->
    Ref = make_ref(),
    case emdb_drv:dummy_action(Ref, Handle) of
	ok ->
	    receive_answer(Ref, ?EMDB_TIMEOUT);
	V ->
	    V
    end.

bulk_txn_prepared(List) when is_list(List) ->
    bulk_txn_helper(List).

%%
%% Will execute a bulk transaction, writing and reading the same
%% value within the bulk transaction will happen in order.
%%
%% The argument is a list with entries of
%% {get, Key}
%% {update, {Key, Value}}
%% {del, Key}
%% The responses are the same as the respective functions have and
%% will be returned in a list that has the same order as the requests.
%%
%% For performance reasons Entries can be prepared in Erlang
%% beforehand with prepare_bulk/1.
%% And transaction handles can be accuired beforehand.
%% A bulk transaction will close or open and close a transaction.

bulk_txn(List) when is_list(List) ->
    bulk_txn_helper([prepare_bulk(Entry) || Entry <- List]).

bulk_txn_helper(PreparedList) ->
    Ref = make_ref(),
    case emdb_drv:bulk_txn(Ref, Handle, PreparedList) of
	ok ->
	    case receive_answer(Ref, ?EMDB_TIMEOUT) of
		{ok, Replies} when is_list(Replies) ->
		    {ok, parse_bulk_replies(Replies, PreparedList)};
		V ->
		    V
	    end;
	V ->
	    V
    end.


parse_bulk_replies([RH|RT], [{get, _}|T]) ->
    [parse_bulk_get(RH) | parse_bulk_replies(RT, T)];

parse_bulk_replies([RH|RT], [{del, _}|T]) ->
    [parse_bulk_del(RH) | parse_bulk_replies(RT, T)];

parse_bulk_replies([RH|RT], [{update, _}|T]) ->
    [parse_bulk_update(RH) | parse_bulk_replies(RT, T)];

parse_bulk_replies([], []) ->
    [].


parse_bulk_get({ok,  V}) ->
    {ok, binary_to_term(V)};

parse_bulk_get({error, _} =V) ->
    V.


parse_bulk_update(ok) ->
    ok;
parse_bulk_update({error, _} =V) ->
    V.


parse_bulk_del(V) ->
    ok;
parse_bulk_del({error, _} =V) ->
    V.


prepare_bulk({get, V}) ->
    {get, {term_to_binary(V)}};
prepare_bulk({update, {K, V}}) ->
    {update, {term_to_binary(K), term_to_binary(V)}};
prepare_bulk({del, K}) ->
    {del, {term_to_binary(K)}}.


%%====================================================================
%% INTERNAL FUNCTIONS
%%====================================================================

receive_answer(Ref, _Timeout) ->
    receive 
        {Ref, Resp} -> Resp
   % after Timeout ->
   %     {error, timeout}
    end.
