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
    {ok, Handle} = emdb:open("./emdb_test", 104857600),
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
    none = Handle:get(<<"a">>),
    ok = Handle:put(<<"a">>, <<"17">>),
    {ok, <<"17">>} = Handle:get(<<"a">>),
    ok = Handle:update(<<"a">>, <<"hello world">>),
    {ok, <<"hello world">>} = Handle:get(<<"a">>),
    ok = Handle:del(<<"a">>),
    none = Handle:get(<<"a">>),
    ok.

cursor_order() ->
    [].

cursor_order(Config) ->
    apply_to_inputs([vnode1], lists:seq(1,2), lists:seq(1,10),
		    fun(A,B,C) ->
			    error_logger:info_msg("trying to store: ~p~n", [{A,B,C}])
		    end),
    error_logger:info_msg("applied to inputs~n", []),
    Handle = proplists:get_value(handle, Config),
    VNodes = [vnode1],
    Groups = lists:seq(1,10),
    LogEntries = lists:seq(1,10000),
    Entrys = lists:flatten([[[{VNode, Num, Idx} || Idx <- LogEntries] || Num <- Groups] || VNode <- VNodes]),
    ok = Handle:txn_begin(),
    apply_to_inputs(VNodes, Groups, LogEntries, 
		    fun(A, B, C) ->
			    case Handle:put({A,B,C}, 1) of
				error_txn_full ->
				    Handle:txn_commit(),
				    Handle:txn_begin(),
				    Handle:put({A,B,C},1);
				ok -> ok
			    end
		    end),
    ok = Handle:txn_commit(),
    %% {Time, _} = timer:tc(fun() -> 
    %% 				 ok = Handle:txn_begin(),
    %% 				 [case Handle:put(E, 1) of
    %% 				      error_txn_full ->
    %% 					  Handle:txn_commit(),
    %% 					  Handle:txn_begin(),
    %% 					  Handle:put(E,1);
    %% 				      ok -> ok
    %% 				  end || E <- lists:reverse(Entrys)],
    %% 				 ok = Handle:txn_commit() 
    %% 			 end),
    error_logger:info_msg("added entries, took: ~p~n", [{0}]),
    Handle:cursor_open(),
%    apply_to_inputs(),
    walk_db(Handle, Entrys),
    error_logger:info_msg("walked db~n", []),
    error_cursor_get = Handle:cursor_next(),
    {_, [H|LastEntrys]} = lists:split(10, Entrys),
    {ok, {H, _}} = Handle:cursor_set(H),
    walk_db(Handle, LastEntrys),
    error_logger:info_msg("walked db again~n", []),
    error_cursor_get = Handle:cursor_next(),
    {ok, {H, _}} = Handle:cursor_set(H),
    ok = Handle:cursor_close(),
    ok = Handle:cursor_open(),
    {ok, _} = Handle:cursor_next(),
    error_logger:info_msg("starting with deletion~n", []),
    ok = delete_upto(Handle, H),
    ok = Handle:cursor_close(),
    Handle:cursor_open(),
    walk_db(Handle,[H|LastEntrys]),    
    error_cursor_set = Handle:cursor_set(hd(Entrys)),
    Handle:cursor_close(). 

delete_upto(Handle, DontDelete) ->
    case Handle:cursor_del() of
	{ok, {DontDelete, _}} ->
	    error_logger:info_msg("deleted upto: ~p~n", [DontDelete]),
	    ok;
	{ok, {_K, _}} ->
	    error_logger:error_msg("in delete_upto, deleted ~p~n", [_K]),
	    delete_upto(Handle, DontDelete);
	V ->
	    error_logger:error_msg("in delete_upto, got unhandled case clause: ~p", [V]),
	    ok
    end.    

apply_to_inputs(D1, D2, D3, Fun) ->
    HelperF = fun(F, [H1|T1], [H2|T2], [H3|T3]) ->
		      Fun(H1, H2, H3),
		      F(F, [H1|T1], [H2|T2], T3);
		 (F, [H1|[]], [H2|[]], []) ->
		      ok;
		 (F, [H1|T1], [H2|[]], []) ->
		      F(F, T1, D2, D3);
		 (F, L1, [H2|T2], []) ->
		      F(F, L1, T2, D3);
		 (F, [H1|T1], [], []) ->
		      F(F, T1, D2, D3)
	      end,
    HelperF(HelperF, D1, D2, D3).

walk_db(_Handle, []) ->
    ok;

walk_db(Handle, [H|T]) ->
    case Handle:cursor_next() of
	error_cursor_get ->
	    1=2;
	{ok, {H, _}} ->
	    walk_db(Handle, T);
	V ->
	    error_logger:error_msg("in walk_db, got unhandled case clause: ~p, expected:~p~n", [V, H]),
	    ok
    end.
