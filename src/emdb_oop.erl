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
	 txn_commit/0,

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
%% @doc 
%% @end
%%--------------------------------------------------------------------
close() ->
    emdb_drv:close(Handle).

%%--------------------------------------------------------------------
%% @doc 
%% @end
%%--------------------------------------------------------------------
put(Key, Val) ->%when is_binary(Key) andalso is_binary(Val) ->
    emdb_drv:put(Handle, term_to_binary(Key), term_to_binary(Val)).


append(Key, Val) ->%when is_binary(Key) andalso is_binary(Val) ->
    emdb_drv:append(Handle, term_to_binary(Key), term_to_binary(Val)).

%%--------------------------------------------------------------------
%% @doc 
%% @end
%%--------------------------------------------------------------------
get(Key)->% when is_binary(Key) ->
    case emdb_drv:get(Handle, term_to_binary(Key)) of
	{ok, V} ->
	    {ok, binary_to_term(V)};
	V ->
	    V
    end.

%%--------------------------------------------------------------------
%% @doc 
%% @end
%%--------------------------------------------------------------------
del(Key) ->% when is_binary(Key) ->
    emdb_drv:del(Handle, term_to_binary(Key)).

%%--------------------------------------------------------------------
%% @doc 
%% @end
%%--------------------------------------------------------------------
update(Key, Val) ->%when is_binary(Key) andalso is_binary(Val) ->
    emdb_drv:update(Handle, term_to_binary(Key), term_to_binary(Val)).


cursor_open() ->
    emdb_drv:cursor_open(Handle).

cursor_close() ->
    emdb_drv:cursor_close(Handle).

cursor_next() ->
    case emdb_drv:cursor_next(Handle) of
	{ok, {K,V}} ->
	    {ok, {binary_to_term(K), binary_to_term(V)}};
	V ->
	    V
    end.

cursor_set(Key) ->
    case emdb_drv:cursor_set(Handle, term_to_binary(Key)) of
	{ok, {K,V}} ->
	    {ok, {binary_to_term(K), binary_to_term(V)}};
	V ->
	    V
    end.

cursor_del() ->
    case emdb_drv:cursor_del(Handle) of
	{ok, {K,V}} ->
	    {ok, {binary_to_term(K), binary_to_term(V)}};
	V ->
	    V
    end.

txn_begin() ->
    emdb_drv:txn_begin(Handle).

txn_commit() ->
    emdb_drv:txn_commit(Handle).

%%--------------------------------------------------------------------
%% @doc 
%% @end
%%--------------------------------------------------------------------
drop() ->
    emdb_drv:drop(Handle).

%%====================================================================
%% INTERNAL FUNCTIONS
%%====================================================================
