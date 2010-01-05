%% -------------------------------------------------------------------
%%
%% innostore: Simple Erlang API to Embedded Inno DB
%%
%% Copyright (c) 2009 Basho Technologies, Inc. All Rights Reserved.
%%
%% innostore is free software: you can redistribute it and/or modify
%% it under the terms of the GNU General Public License as published by
%% the Free Software Foundation, either version 2 of the License, or
%% (at your option) any later version.
%%
%% innostore is distributed in the hope that it will be useful,
%% but WITHOUT ANY WARRANTY; without even the implied warranty of
%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
%% GNU General Public License for more details.
%%
%% You should have received a copy of the GNU General Public License
%% along with innostore.  If not, see <http://www.gnu.org/licenses/>.
%%
%% -------------------------------------------------------------------
-module(innostore).

-author('Dave Smith <dizzyd@basho.com>').

%% Public API
-export([connect/0,
         disconnect/1,
         open_keystore/2,
         list_keystores/1,
         drop_keystore/2,
         get/2,
         put/3,
         delete/2,
         fold_keys/3,
         fold/3]).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(CMD_SET_CFG,              1).
-define(CMD_START,          1 bsl 1).
-define(CMD_INIT_TABLE,     1 bsl 2).
-define(CMD_IS_STARTED,     1 bsl 3).
-define(CMD_GET,            1 bsl 4).
-define(CMD_PUT,            1 bsl 5).
-define(CMD_DELETE,         1 bsl 6).
-define(CMD_LIST_TABLES,    1 bsl 7).
-define(CMD_CURSOR_OPEN,    1 bsl 8).
-define(CMD_CURSOR_MOVE,    1 bsl 9).
-define(CMD_CURSOR_CLOSE,   1 bsl 10).
-define(CMD_DROP_TABLE,     1 bsl 11).

-define(CURSOR_FIRST, 0).
-define(CURSOR_NEXT,  1).
-define(CURSOR_PREV,  2).
-define(CURSOR_LAST,  3).

-define(CONTENT_KEY_ONLY,  0).
-define(CONTENT_KEY_VALUE, 1).

-define(COMPRESSION_NONE, 0).

-record(store, { port,
                 table_id,
                 compression = ?COMPRESSION_NONE }).


%% ===================================================================
%% Public API
%% ===================================================================

connect() ->
    case erl_ddll:load_driver(priv_dir(), innostore_drv) of
        Res when Res == ok; Res == {error, permanent} ->
            Port = open_port({spawn, innostore_drv}, [binary]),
            case is_started(Port) of
                true ->
                    {ok, Port};
                false ->
                    ensure_app_loaded(),
                    set_config(application:get_all_env(innostore), Port),
                    erlang:port_control(Port, ?CMD_START, <<>>),
                    receive
                        innostore_ok ->
                            {ok, Port};
                        {innostore_error, Reason} ->
                            {error, Reason}
                    end
            end;

        {error, LoadError} ->
            Str = erl_ddll:format_error(LoadError),
            error_logger:error_msg("Error loading driver ~s: ~p\n", [?MODULE, Str]),
            throw({error, {LoadError, Str}})
    end.

disconnect(Port) ->
    port_close(Port),
    ok.

open_keystore(Name, Port) when is_atom(Name) ->
    open_keystore(atom_to_binary(Name, utf8), Port);
open_keystore(Name, Port) when is_list(Name) ->
    open_keystore(list_to_binary(Name), Port);
open_keystore(Name, Port) when is_binary(Name) ->
    TableName = <<"innokeystore/", Name/binary>>,
    erlang:port_control(Port, ?CMD_INIT_TABLE, <<TableName/binary, 0:8>>),
    receive
        {innostore_ok, <<_:64/unsigned-native>> = TableId} ->
            {ok, #store { port = Port,
                          table_id = TableId }};

        {innostore_error, Reason}->
            {error, Reason}
    end.

list_keystores(Port) ->
    erlang:port_control(Port, ?CMD_LIST_TABLES, <<>>),
    list_keystores_loop([]).


drop_keystore(Name, Port) when is_atom(Name) ->
    drop_keystore(atom_to_binary(Name, utf8), Port);
drop_keystore(Name, Port) when is_list(Name) ->
    drop_keystore(list_to_binary(Name), Port);
drop_keystore(Name, Port) ->
    TableName = <<"innokeystore/", Name/binary>>,
    erlang:port_control(Port, ?CMD_DROP_TABLE, <<TableName/binary, 0:8>>),
    receive
        innostore_ok ->
            ok;
        {innostore_error, Reason} ->
            {error, Reason}
    end.


get(Key, Store) ->
    Args = <<(Store#store.table_id)/binary, (size(Key)):8, Key/binary>>,
    erlang:port_control(Store#store.port, ?CMD_GET, Args),
    receive
        {innostore_ok, not_found} ->
            {ok, not_found};
        {innostore_ok, Value} ->
            {ok, Value};
        {innostore_error, Reason} ->
            {error, Reason}
    end.


put(Key, Value, Store) ->
    Args = <<(Store#store.table_id)/binary, (Store#store.compression):8,
            (size(Key)):8, Key/binary,
            (size(Value)):32/native, Value/binary>>,
    erlang:port_control(Store#store.port, ?CMD_PUT, Args),
    receive
        innostore_ok ->
            ok;
        {innostore_error, Reason} ->
            {error, Reason}
    end.

delete(Key, Store) ->
    Args = <<(Store#store.table_id)/binary, (size(Key)):8, Key/binary>>,
    erlang:port_control(Store#store.port, ?CMD_DELETE, Args),
    receive
        innostore_ok ->
            ok;
        {innostore_error, Reason} ->
            {error, Reason}
    end.

fold_keys(Fun, Acc0, Store) ->
    fold(Fun, Acc0, ?CONTENT_KEY_ONLY, Store).

fold(Fun, Acc0, Store) ->
    fold(Fun, Acc0, ?CONTENT_KEY_VALUE, Store).


%% ===================================================================
%% Internal functions
%% ===================================================================

priv_dir() ->
    case code:priv_dir(?MODULE) of
        Name when is_list(Name) ->
            Name;
        {error, bad_name} ->
            {ok, Cwd} = file:get_cwd(),
            filename:absname(filename:join(Cwd, "../priv"))
    end.

is_started(Port) ->
    erlang:port_control(Port, ?CMD_IS_STARTED, <<>>) == <<1>>.

ensure_app_loaded() ->
    case lists:keymember(?MODULE, 1, application:loaded_applications()) of
        true ->
            ok;
        false ->
            case application:load(?MODULE) of
                ok ->
                    ok;
                Other ->
                    error_logger:info_msg("Innostore app config NOT loaded; default config in use: ~p\n", [Other])
            end
    end.

set_config([], _Port) ->
    ok;
set_config([{Key, Value} | Rest], Port) when is_atom(Key) ->
    case lists:keysearch(Key, 1, config_types()) of
        {value, {Key, Type}} ->
            KBin = atom_to_binary(Key, utf8),
            VBin = config_encode(Type, Value),
            erlang:port_control(Port, ?CMD_SET_CFG, <<KBin/binary, 0:8, VBin/binary>>),
            receive
                innostore_ok ->
                    ok;
                {innostore_error, Reason} ->
                    error_logger:error_msg("Failed to set value for ~p = ~p: ~p\n", [Key, Value, Reason])
            end;
        false ->
            error_logger:error_msg("Skipping config setting ~p; unknown option.\n", [Key])
    end,
    set_config(Rest, Port);
set_config([Other | Rest], Port) ->
    error_logger:info_msg("Skipping config setting ~p for innostore; not {atom, list} pair.\n",
                          [Other]),
    set_config(Rest, Port).


list_keystores_loop(Acc) ->
    receive
        {innostore_table_name, "innokeystore/" ++ Table} ->
            list_keystores_loop([Table | Acc]);
        innostore_ok ->
            lists:reverse(Acc);
        {innostore_error, Reason} ->
            {error, Reason}
    end.


cursor_open(Store) ->
    erlang:port_control(Store#store.port, ?CMD_CURSOR_OPEN, <<(Store#store.table_id)/binary>>),
    receive
        innostore_ok ->
            ok;
        {innostore_error, Reason} ->
            {error, Reason}
    end.

cursor_move(Direction, Content, Store) ->
    erlang:port_control(Store#store.port, ?CMD_CURSOR_MOVE, <<Direction:8, Content:8>>),
    receive
        {innostore_ok, Key} ->
            {ok, Key};
        {innostore_ok, Key, Value} ->
            {ok, Key, Value};
        {innostore_error, Reason} ->
            {error, Reason}
    end.

cursor_close(Store) ->
    erlang:port_control(Store#store.port, ?CMD_CURSOR_CLOSE, <<>>),
    receive
        innostore_ok ->
            ok;
        {innostore_error, Reason} ->
            {error, Reason}
    end.

fold(Fun, Acc0, Content, Store) ->
    case cursor_open(Store) of
        ok ->
            case fold_loop(?CURSOR_FIRST, Content, Fun, Acc0, Store) of
                {ok, Acc} ->
                    cursor_close(Store),
                    Acc;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

fold_loop(Direction, Content, Fun, Acc, Store) ->
    case cursor_move(Direction, Content, Store) of
        {ok, eof} ->
            {ok, Acc};
        {ok, Key} ->
            Acc1 = Fun(Key, Acc),
            fold_loop(?CURSOR_NEXT, Content, Fun, Acc1, Store);
        {ok, Key, Value} ->
            Acc1 = Fun(Key, Value, Acc),
            fold_loop(?CURSOR_NEXT, Content, Fun, Acc1, Store);
        {error, Reason} ->
            {error, Reason}
    end.


%%
%% Configuration type information. Extracted from api/api0cfg.c in inno distribution.
%%
config_types() ->
    [{adaptive_hash_index, bool},
     {additional_mem_pool_size, integer},
     {autoextend_increment, integer},
     {buffer_pool_size, integer},
     {checksums, bool},
     {concurrency_tickets, integer},
     {data_file_path, string},
     {data_home_dir, string},
     {doublewrite, bool},
     {fast_shutdown, integer},
     {file_format, string},
     {file_io_threads, integer},
     {file_per_table, bool},
     {flush_log_at_trx_commit, integer},
     {flush_method, string},
     {force_recovery, integer},
     {lock_wait_timeout, integer},
     {log_buffer_size, integer},
     {log_file_size, integer},
     {log_files_in_group, integer},
     {log_group_home_dir, string},
     {max_dirty_pages_pct, integer},
     {max_purge_lag, integer},
     {mirrored_log_groups, integer},
     {open_files, integer},
     {print_verbose_log, bool},
     {rollback_on_timeout, bool},
     {stats_sample_pages, integer},
     {status_file, bool},
     {sync_spin_loops, integer},
     {thread_concurrency, integer},
     {thread_sleep_delay, integer},
     {version, string}].

%%
%% Encode configuration setting, based on type for passing through to inno api
%%
config_encode(integer, Value) ->
    case erlang:system_info(wordsize) of
        4 -> <<Value:32/unsigned-native>>;
        8 -> <<Value:64/unsigned-native>>
    end;
config_encode(bool, true) ->
    config_encode(integer, 1);
config_encode(bool, false) ->
    config_encode(integer, 0);
config_encode(string, Value) ->
    <<(list_to_binary(Value))/binary, 0:8>>.



%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

connect_reset() ->
    {ok, Port} = connect(),
    [ok = drop_keystore(T, Port) || T <- list_keystores(Port)],
    {ok, Port}.

startup_test() ->
    {ok, Port} = connect(),
    true = is_started(Port),
    {ok, Port2} = connect(),
    true = is_started(Port2).

roundtrip_test() ->
    ok = roundtrip_test_op(?COMPRESSION_NONE).

roundtrip_test_op(Compression) ->
    {ok, Port} = connect_reset(),
    {ok, Store} = open_keystore(test, Port),
    S2 = Store#store { compression = Compression },
    ok = ?MODULE:put(<<"key1">>, <<"value1">>, S2),
    {ok, <<"value1">>} = ?MODULE:get(<<"key1">>, S2),
    ok = ?MODULE:delete(<<"key1">>, S2),
    {ok, not_found} = ?MODULE:get(<<"key1">>, S2),
    ok.

list_tables_test() ->
    {ok, Port} = connect_reset(),
    {ok, _} = open_keystore(foobar, Port),
    {ok, _} = open_keystore(barbaz, Port),
    {ok, _} = open_keystore(bazbaz, Port),
    ["barbaz", "bazbaz", "foobar"] = lists:sort(list_keystores(Port)).

-endif.
