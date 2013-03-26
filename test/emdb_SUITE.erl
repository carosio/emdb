%%%-------------------------------------------------------------------
%%% @author olerixmanntp <olerixmanntp@kiiiiste>
%%% @copyright (C) 2013, olerixmanntp
%%% @doc
%%%
%%% @end
%%% Created : 20 Mar 2013 by olerixmanntp <olerixmanntp@kiiiiste>
%%%-------------------------------------------------------------------
-module(emdb_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% @spec suite() -> Info
%% Info = [tuple()]
%% @end
%%--------------------------------------------------------------------
suite() ->
    [{timetrap,{seconds,3000}}].

%%--------------------------------------------------------------------
%% @spec init_per_suite(Config0) ->
%%     Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% @end
%%--------------------------------------------------------------------
init_per_suite(Config) ->
    Config.

%%--------------------------------------------------------------------
%% @spec end_per_suite(Config0) -> void() | {save_config,Config1}
%% Config0 = Config1 = [tuple()]
%% @end
%%--------------------------------------------------------------------
end_per_suite(_Config) ->
    ok.

%%--------------------------------------------------------------------
%% @spec init_per_group(GroupName, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% GroupName = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% @end
%%--------------------------------------------------------------------
init_per_group(_GroupName, Config) ->
    Config.

%%--------------------------------------------------------------------
%% @spec end_per_group(GroupName, Config0) ->
%%               void() | {save_config,Config1}
%% GroupName = atom()
%% Config0 = Config1 = [tuple()]
%% @end
%%--------------------------------------------------------------------
end_per_group(_GroupName, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% @spec init_per_testcase(TestCase, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% TestCase = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% @end
%%--------------------------------------------------------------------
init_per_testcase(_TestCase, Config) ->
    {ok, Handle} = emdb:open("./emdb_test", 104857600000),
    [{handle, Handle} | Config].

%%--------------------------------------------------------------------
%% @spec end_per_testcase(TestCase, Config0) ->
%%               void() | {save_config,Config1} | {fail,Reason}
%% TestCase = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% @end
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, Config) ->
    error_logger:info_msg("in end_per_testcase~n", []),
    Handle = proplists:get_value(handle, Config),
    Handle:drop(),
    Handle:close(),
    ok.

%%--------------------------------------------------------------------
%% @spec groups() -> [Group]
%% Group = {GroupName,Properties,GroupsAndTestCases}
%% GroupName = atom()
%% Properties = [parallel | sequence | Shuffle | {RepeatType,N}]
%% GroupsAndTestCases = [Group | {group,GroupName} | TestCase]
%% TestCase = atom()
%% Shuffle = shuffle | {shuffle,{integer(),integer(),integer()}}
%% RepeatType = repeat | repeat_until_all_ok | repeat_until_all_fail |
%%              repeat_until_any_ok | repeat_until_any_fail
%% N = integer() | forever
%% @end
%%--------------------------------------------------------------------
groups() ->
    [].


all() -> 
    [emdb_basic, cursor_order].


emdb_basic() -> 
    [].

emdb_basic(Config) -> 
    Handle = proplists:get_value(handle, Config),
    undefined = Handle:get(<<"a">>),
    ok = Handle:put(<<"a">>, <<"17">>),
    {ok, <<"17">>} = Handle:get(<<"a">>),
    ok = Handle:update(<<"a">>, <<"hello world">>),
    {ok, <<"hello world">>} = Handle:get(<<"a">>),
    ok = Handle:del(<<"a">>),
    undefined = Handle:get(<<"a">>),
    ok.

cursor_order() ->
    [].

cursor_order(Config) ->
    Handle = proplists:get_value(handle, Config),
    VNodes = [vnode1],
    Groups = lists:seq(1,100),
    LogEntries = lists:seq(1,100000),
    ok = Handle:txn_begin(), 
    error_logger:info_msg("starting to add entries!~n", []),
    {Time, _} = timer:tc(emdb_SUITE, apply_to_inputs, [VNodes, Groups, LogEntries, 
		    fun(A, B, C, I) ->
			    Remainder = I rem 100000,
			    case  Remainder of
				0 ->
				    error_logger:info_msg("added ~pM messages~n", [I div 1000000]),
				    ok = Handle:txn_commit(),
				    ok = Handle:txn_begin(),
				    ok = Handle:put({A,B,C},1);
				_ ->
				    ok = Handle:put({A,B,C}, 1)
			    end
		    end]),
    error_logger:info_msg("added entries, took ~p~n", [Time]),
    ok = Handle:txn_commit(),

    ok = Handle:txn_begin_ro(), %starting read-only transaction
    {error, error_put} = Handle:put(a, "hello_world"),

    ok = Handle:cursor_open(),

    apply_to_inputs(VNodes, Groups, LogEntries,
		    fun(A,B,C,_) ->
			    {ok, {{A,B,C}, _}} = Handle:cursor_next()
		    end),
    error_logger:info_msg("walked db~n", []),

    {error, error_cursor_get} = Handle:cursor_next(),
    {_, LastGroups} = lists:split(5, Groups),
    {_, LastLogEntries} = lists:split(5, LogEntries),
    FirstNonDeletedKey = {hd(VNodes), hd(LastGroups), hd(LastLogEntries) - 1},
    {ok, {_, _}} = Handle:cursor_set(FirstNonDeletedKey),
    apply_to_inputs(VNodes, Groups, LogEntries,
		    fun(A,B,C,_) ->
			    {ok, {{A,B,C}, _}} = Handle:cursor_next()
		    end, 
		    VNodes, LastGroups, LastLogEntries),
    error_logger:info_msg("walked db again~n", []),
    {error, error_cursor_get} = Handle:cursor_next(),
    {ok, {FirstNonDeletedKey, _}} = Handle:cursor_set(FirstNonDeletedKey),
    ok = Handle:cursor_close(), %read-only transaction is closed

    ok = Handle:cursor_open(),
    {ok, _} = Handle:cursor_next(),
    error_logger:info_msg("starting with deletion~n", []),
    ok = delete_upto(Handle, FirstNonDeletedKey), % deleting first part of db
    ok = Handle:cursor_close(),
    ok = Handle:cursor_open(),
    error_logger:info_msg("checking the non-deleted part of the db~n", []),
    {ok, {FirstNonDeletedKey, _}} = Handle:cursor_next(), % checking the remaining db
    apply_to_inputs(VNodes, Groups, LogEntries,
		    fun(A,B,C,_) ->
			    {ok, {{A,B,C}, _}} = Handle:cursor_next()
		    end, 
		    VNodes, LastGroups, LastLogEntries), 
    error_logger:info_msg("checked the non-deleted part of the db~n", []),
    {error, error_cursor_set} = Handle:cursor_set({hd(VNodes), hd(Groups), hd(LogEntries)}),
    error_logger:info_msg("everything ok, only closing the cursor and txn~n", []),
    ok = Handle:cursor_close(),
    error_logger:info_msg("closed the cursor and txn~n", []),
    timer:sleep(10000). 

delete_upto(Handle, DontDelete) ->
    case Handle:cursor_del() of
	{ok, {DontDelete, _}} ->
	    error_logger:info_msg("deleted upto: ~p~n", [DontDelete]),
	    ok;
	{ok, {_K, _}} ->
	    delete_upto(Handle, DontDelete);
	V ->
	    error_logger:error_msg("in delete_upto, got unhandled case clause: ~p", [V]),
	    ok
    end.    

apply_to_inputs(D1, D2, D3, Fun) ->
    apply_to_inputs(D1, D2, D3, Fun, D1, D2, D3).

apply_to_inputs(D1, D2, D3, Fun, D1I, D2I, D3I) ->
    apply_to_inputs(D1, D2, D3, Fun, D1I, D2I, D3I, 0).
apply_to_inputs(D1, D2, D3, Fun, D1I, D2I, D3I, InitI) ->
    HelperF = fun(F, [H1|T1], [H2|T2], [H3|T3], I) ->
		      Fun(H1, H2, H3, I),
		      F(F, [H1|T1], [H2|T2], T3, I + 1);
		 (F, [H1|[]], [H2|[]], [], I) ->
		      ok;
		 (F, [H1|T1], [H2|[]], [], I) ->
		      F(F, T1, D2, D3, I + 1);
		 (F, L1, [H2|T2], [], I) ->
		      F(F, L1, T2, D3, I + 1);
		 (F, [H1|T1], [], [], I) ->
		      F(F, T1, D2, D3, I + 1)
	      end,
    HelperF(HelperF, D1I, D2I, D3I, InitI).

walk_db(_Handle, []) ->
    ok;

walk_db(Handle, [H|T]) ->
    case Handle:cursor_next() of
	{error, error_cursor_get} ->
	    1=2;
	{ok, {H, _}} ->
	    walk_db(Handle, T);
	V ->
	    error_logger:error_msg("in walk_db, got unhandled case clause: ~p, expected:~p~n", [V, H]),
	    ok
    end.
