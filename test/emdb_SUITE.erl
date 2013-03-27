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
    Handle = proplists:get_value(handle, Config),
    ok = Handle:drop(),
    ok = Handle:close(),
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
    {error, undefined} = Handle:get(<<"a">>),
    ok = Handle:put(<<"a">>, <<"17">>),
    {ok, <<"17">>} = Handle:get(<<"a">>),
    ok = Handle:update(<<"a">>, <<"hello world">>),
    {ok, <<"hello world">>} = Handle:get(<<"a">>),
    ok = Handle:del(<<"a">>),
    {error, undefined} = Handle:get(<<"a">>),
    ok.

cursor_order() ->
    [].

cursor_order(Config) ->
    Handle = proplists:get_value(handle, Config),
    VNodes = [vnode1],
    Groups = lists:seq(1,10),
    LogEntries = lists:seq(1,1000000),
    ok = Handle:txn_begin(), 
    error_logger:info_msg("starting to add entries!~n", []),
    {Time, _} = timer:tc(emdb_SUITE, apply_to_inputs, [VNodes, Groups, LogEntries, 
		    fun(A, B, C, I) ->
			    Remainder = I rem 1000000,
			    case  Remainder of
				0 ->
				    error_logger:info_msg("added ~pM messages~n", [I / 1000000]),
				    ok = Handle:txn_commit(),
				    ok = Handle:txn_begin(),
				    ok = Handle:append({A,B,C},1); 
				_ -> % appending is faster, put can be used
				    ok = Handle:append({A,B,C}, 1)
			    end
		    end]),
    ok = Handle:txn_commit(),
    error_logger:info_msg("added entries, took ~p seconds~n", [Time div 1000000]),

    ok = Handle:txn_begin(), % checking that transaction aborting works
    ok = Handle:put(a, "hello world"),
    ok = Handle:txn_abort(),
    {error, undefined} = Handle:get(a),

    ok = Handle:txn_begin_ro(), % starting read-only transaction
    {error, error_put} = Handle:put(a, "hello_world"),

    ok = Handle:cursor_open(),

    {Time2, _} = timer:tc(emdb_SUITE, apply_to_inputs, [VNodes, Groups, LogEntries,
							fun(A,B,C,_) ->
								{ok, {{A,B,C}, _}} = Handle:cursor_next()
							end]),
    error_logger:info_msg("checked db order, took ~p seconds~n", [Time2 div 1000000]),
    
    {error, error_cursor_get} = Handle:cursor_next(),
    {_, LastGroups} = lists:split(5, Groups),
    {_, LastLogEntries} = lists:split(5, LogEntries),

    FirstNonDeletedKey = {hd(VNodes), hd(LastGroups), hd(LastLogEntries) - 1},

    {ok, {FirstNonDeletedKey, _}} = Handle:cursor_set(FirstNonDeletedKey), 
    ok = Handle:cursor_close(), % read-only transaction is closed

    ok = Handle:cursor_open(), % starting transaction to delete parts of db
    {ok, _} = Handle:cursor_next(), % moving to the first item to delete

    error_logger:info_msg("deleting first part of the db~n", []),
    Time3Start = now(),
    ok = delete_upto(Handle, FirstNonDeletedKey), 
    ok = Handle:cursor_close(),
    Time3Stop = now(),
    Time3 = timer:now_diff(Time3Stop, Time3Start),
    error_logger:info_msg("deleted first part of db, took ~p seconds~n", [Time3 div 1000000]),
    
    ok = Handle:cursor_open(),
    {ok, {FirstNonDeletedKey, _}} = Handle:cursor_next(), % checking the remaining db
    {Time4, ok} = timer:tc(emdb_SUITE, apply_to_inputs, [VNodes, Groups, LogEntries,
							 fun(A,B,C,_) ->
								 {ok, {{A,B,C}, _}} = Handle:cursor_next()
							 end, 
							 VNodes, LastGroups, LastLogEntries]), 
    error_logger:info_msg("checked the remaining part of the db, took ~p seconds~n", [Time4 div 1000000]),

    {error, error_cursor_set} = Handle:cursor_set({hd(VNodes), hd(Groups), hd(LogEntries)}),

    ok = Handle:cursor_close(),

    timer:sleep(10000). 



%% deletes the whole database, starting from the current cursor position, stopping
%% when running into DontDelete

delete_upto(Handle, DontDelete) ->
    case Handle:cursor_del() of
	{ok, {DontDelete, _}} ->
	    ok;
	{ok, {_K, _}} ->
	    delete_upto(Handle, DontDelete);
	V ->
	    error_logger:error_msg("in delete_upto, got unhandled case clause: ~p", [V]),
	    ok
    end.    



%% Applys Fun to each element of the Finite Space D1xD2xD3
%% in the same order that is used by mdb (erlang term order).
%% D1-3 must be provided in the form of lists, containing all the subspaces elements.

%% Fun gets 4 args d1, d2, d3 and i where i counts the calls to Fun.
%% If called with 7 args, the starting element is defined by the last 3.

apply_to_inputs(D1, D2, D3, Fun) ->
    apply_to_inputs(D1, D2, D3, Fun, D1, D2, D3).

apply_to_inputs(D1, D2, D3, Fun, D1I, D2I, D3I) ->
    apply_to_inputs(D1, D2, D3, Fun, D1I, D2I, D3I, 0).

apply_to_inputs(D1, D2, D3, Fun, D1I, D2I, D3I, InitI) when is_list(D1), is_list(D2), is_list(D3), is_function(Fun) -> 
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

