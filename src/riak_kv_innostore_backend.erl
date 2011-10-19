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
-ifndef(OVERRIDE_MODULE).
-module(riak_kv_innostore_backend).
-endif.

-author('Dave Smith <dizzyd@basho.com>').

%% KV Backend API
-export([api_version/0,
         start/2,
         stop/1,
         get/3,
         put/5,
         delete/4,
         drop/1,
         fold_buckets/4,
         fold_keys/4,
         fold_objects/4,
         is_empty/1,
         status/1,
         callback/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(API_VERSION, 1).
-define(CAPABILITIES, [async_fold]).

-record(state, { partition_str,
                 port }).

-opaque(state() :: #state{}).
-type config() :: [{atom(), term()}].
-type bucket() :: binary().
-type key() :: binary().
-type fold_buckets_fun() :: fun((binary(), any()) -> any() | no_return()).
-type fold_keys_fun() :: fun((binary(), binary(), any()) -> any() |
                                                            no_return()).
-type fold_objects_fun() :: fun((binary(), binary(), term(), any()) ->
                                       any() |
                                       no_return()).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Return the major version of the
%% current API and a capabilities list.
-spec api_version() -> {integer(), [atom()]}.
api_version() ->
    {?API_VERSION, ?CAPABILITIES}.

%% @doc Start the innostore backend
-spec start(integer(), config()) -> {ok, state()} | {error, term()}.
start(Partition, _Config) ->
    case innostore:connect() of
        {ok, Port} ->
            PartitionStr = <<"_", (list_to_binary(integer_to_list(Partition)))/binary>>,
            {ok, #state { partition_str = PartitionStr, port = Port }};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Stop the innostore backend
-spec stop(state()) -> ok.
stop(State) ->
    innostore:disconnect(State#state.port).

%% @doc Retrieve an object from the innostore backend
-spec get(bucket(), key(), state()) ->
                 {ok, any(), state()} |
                 {ok, not_found, state()} |
                 {error, term(), state()}.
get(Bucket, Key, #state{partition_str=Partition,
                        port=Port}=State) ->
    case innostore:get(Key, keystore(Bucket, Partition, Port)) of
        {ok, not_found} ->
            {error, not_found, State};
        {ok, Value} ->
            {ok, Value, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Insert an object into the innostore backend.
%% NOTE: The innostore backend does not currently support
%% secondary indexing and the _IndexSpecs parameter
%% is ignored.
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) ->
                 {ok, state()} |
                 {error, term(), state()}.
put(Bucket, Key, _IndexSpecs, Value, #state{partition_str=Partition,
                               port=Port}=State) ->
    KeyStore = keystore(Bucket, Partition, Port),
    case innostore:put(Key, Value, KeyStore) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Delete an object from the innostore backend
%% NOTE: The innostore backend does not currently support
%% secondary indexing and the _IndexSpecs parameter
%% is ignored.
-spec delete(bucket(), key(), [index_spec()], state()) ->
                    {ok, state()} |
                    {error, term(), state()}.
delete(Bucket, Key, _IndexSpecs, #state{partition_str=Partition,
                                        port=Port}=State) ->
    KeyStore = keystore(Bucket, Partition, Port),
    case innostore:delete(Key, KeyStore) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Fold over all the buckets.
-spec fold_buckets(fold_buckets_fun(),
                   any(),
                   [],
                   state()) -> {ok, any()} | {async, fun()} | {error, term()}.
fold_buckets(FoldBucketsFun, Acc, Opts, #state{partition_str=Partition,
                                               port=Port}) ->
    FoldFun = fold_buckets_fun(FoldBucketsFun),
    Suffix = binary_to_list(Partition),
    FilterFun =
        fun(KeyStore, Acc1) ->
                case lists:suffix(Suffix, KeyStore) of
                    true ->
                        Bucket = bucket_from_tablename(KeyStore),
                        FoldFun(Bucket, Acc1);
                    false ->
                        Acc1
                end
        end,
    case lists:member(async_fold, Opts) of
        true ->
            BucketFolder =
                fun() ->
                        case innostore:connect() of
                            {ok, Port1} ->
                                FoldResults =
                                    lists:foldl(FilterFun,
                                                Acc,
                                                innostore:list_keystores(Port1)),
                                innostore:disconnect(Port1),
                                FoldResults;
                            {error, Reason} ->
                                {error, Reason}
                        end
                end,
            {async, BucketFolder};
        false ->
            FoldResults = lists:foldl(FilterFun,
                                      Acc,
                                      innostore:list_keystores(Port)),
            {ok, FoldResults}
    end.

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, term()} | {error, term()}.
fold_keys(FoldKeysFun, Acc, Opts, #state{partition_str=Partition,
                                         port=Port}) ->
    Bucket =  proplists:get_value(bucket, Opts),
    case lists:member(async_fold, Opts) of
        true ->
            KeyFolder = async_key_folder(Bucket, FoldKeysFun, Acc, Partition),
            {async, KeyFolder};
        false ->
            FoldResults = sync_key_fold(Bucket, FoldKeysFun, Acc, Partition, Port),
            {ok, FoldResults}
    end.

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {error, term()}.
fold_objects(FoldObjectsFun, Acc, Opts, #state{partition_str=Partition,
                                               port=Port}) ->
    Bucket =  proplists:get_value(bucket, Opts),
    case lists:member(async_fold, Opts) of
        true ->
            KeyFolder = async_object_folder(Bucket, FoldObjectsFun, Acc, Partition),
            {async, KeyFolder};
        false ->
            FoldResults = sync_object_fold(Bucket, FoldObjectsFun, Acc, Partition, Port),
            {ok, FoldResults}
    end.

%% @doc Delete all objects from this innostore backend
-spec drop(state()) -> {ok, state()} | {error, term(), state()}.
drop(#state{partition_str=Partition,
            port=Port}=State) ->
    KeyStores = list_keystores(Partition, Port),
    [innostore:drop_keystore(KeyStore, Port) || KeyStore <- KeyStores],
    {ok, State}.

%% @doc Returns true if this innostore backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> boolean() | {error, term()}.
is_empty(#state{partition_str=Partition,
                port=Port}) ->
    lists:all(fun(I) -> I end,
              [innostore:is_keystore_empty(B, Port) ||
                  B <- list_keystores(Partition, Port)]).

%% @doc Get the status information for this innostore backend
-spec status(state()) -> [{atom(), term()}].
status(#state{port=Port}) ->
    Status = innostore:status(Port),
    format_status(Status).

%% Ignore callbacks we do not know about - may be in multi backend
callback(_Ref, _Msg, State) ->
    {ok, State}.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @private
keystore(Bucket, Partition, Port) ->
    KeyStoreId = <<Bucket/binary, Partition/binary>>,
    case erlang:get({innostore, KeyStoreId}) of
        undefined ->
            {ok, KeyStore} = innostore:open_keystore(KeyStoreId, Port),
            erlang:put({innostore, KeyStoreId}, KeyStore),
            KeyStore;
        KeyStore ->
            KeyStore
    end.

%% @private
%% Return a function to fold over the buckets on this backend
fold_buckets_fun(FoldBucketsFun) ->
    fun(Bucket, Acc) ->
            FoldBucketsFun(Bucket, Acc)
    end.

%% @private
%% Return a function to synchronously fold over keys on this backend
async_key_folder(undefined, FoldFun, Acc, Partition) ->
    fun() ->
            case innostore:connect() of
                {ok, Port} ->
                    Buckets = list_buckets(Partition, Port),
                    %% Fold over all keys in all buckets
                    FoldResults = fold_all_keys(Buckets,
                                                Acc,
                                                FoldFun,
                                                Partition,
                                                Port),
                    innostore:disconnect(Port),
                    FoldResults;
                {error, Reason} ->
                    {error, Reason}
            end
    end;
async_key_folder(Bucket, FoldFun, Acc, Partition) ->
    FoldKeysFun = fold_keys_fun(FoldFun, Bucket),
    fun() ->
            case innostore:connect() of
                {ok, Port} ->
                    KeyStore = keystore(Bucket, Partition, Port),
                    FoldResults =
                        innostore:fold_keys(FoldKeysFun, Acc, KeyStore),
                    innostore:disconnect(Port),
                    FoldResults;
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%% @private
%% Return a function to synchronously fold over keys on this backend
sync_key_fold(undefined, FoldFun, Acc, Partition, Port) ->
    Buckets = list_buckets(Partition, Port),
    %% Fold over all keys in all buckets
    fold_all_keys(Buckets,
                  Acc,
                  FoldFun,
                  Partition,
                  Port);
sync_key_fold(Bucket, FoldFun, Acc, Partition, Port) ->
    FoldKeysFun = fold_keys_fun(FoldFun, Bucket),
    KeyStore = keystore(Bucket, Partition, Port),
    innostore:fold_keys(FoldKeysFun, Acc, KeyStore).

%% @private
fold_keys_fun(FoldKeysFun, Bucket) ->
    fun(Key, Acc) ->
            FoldKeysFun(Bucket, Key, Acc)
    end.

%% @private
%% Return a function to synchronously fold over objects on this backend
sync_object_fold(undefined, FoldFun, Acc, Partition, Port) ->
    Buckets = list_buckets(Partition, Port),
    %% Fold over all keys in all buckets
    fold_all_objects(Buckets,
                     Acc,
                     FoldFun,
                     Partition,
                     Port);
sync_object_fold(Bucket, FoldFun, Acc, Partition, Port) ->
    FoldObjectsFun = fold_objects_fun(FoldFun, Bucket),
    KeyStore = keystore(Bucket, Partition, Port),
    innostore:fold_keys(FoldObjectsFun, Acc, KeyStore).

%% @private
%% Return a function to synchronously fold over objects on this backend
async_object_folder(undefined, FoldFun, Acc, Partition) ->
    fun() ->
            case innostore:connect() of
                {ok, Port} ->
                    Buckets = list_buckets(Partition, Port),
                    %% Fold over all objects in all buckets
                    FoldResults = fold_all_objects(Buckets,
                                                   Acc,
                                                   FoldFun,
                                                   Partition,
                                                   Port),
                    innostore:disconnect(Port),
                    FoldResults;
                {error, Reason} ->
                    {error, Reason}
            end
    end;
async_object_folder(Bucket, FoldFun, Acc, Partition) ->
    FoldObjectsFun = fold_objects_fun(FoldFun, Bucket),
    fun() ->
            case innostore:connect() of
                {ok, Port} ->
                    KeyStore = keystore(Bucket, Partition, Port),
                    FoldResults = innostore:fold(FoldObjectsFun, Acc, KeyStore),
                    innostore:disconnect(Port),
                    FoldResults;
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%% @private
%% Return a function to fold over keys on this backend
fold_objects_fun(FoldObjectsFun, Bucket) ->
    fun(Key, Value, Acc) ->
            FoldObjectsFun(Bucket, Key, Value, Acc)
    end.

%% @private
list_buckets(Partition, Port) ->
    Suffix = binary_to_list(Partition),
    [bucket_from_tablename(KeyStore) || KeyStore <- innostore:list_keystores(Port),
                                        lists:suffix(Suffix, KeyStore) == true].

%% @private
list_keystores(Partition, Port) ->
    Suffix = binary_to_list(Partition),
    [KeyStore || KeyStore <- innostore:list_keystores(Port),
                 lists:suffix(Suffix, KeyStore) == true].

%% @private
fold_all_keys([], Acc, _, _Partition, _Port) ->
    Acc;
fold_all_keys([Bucket | RestBuckets], Acc, FoldKeysFun, Partition, Port) ->
    KeyStore = keystore(Bucket, Partition, Port),
    FoldFun = fold_keys_fun(FoldKeysFun, Bucket),
    case innostore:fold_keys(FoldFun, Acc, KeyStore) of
        {error, Reason} ->
            {error, Reason};
        Acc1 ->
            fold_all_keys(RestBuckets, Acc1, FoldKeysFun, Partition, Port)
    end.

%% @private
fold_all_objects([], Acc, _, _Partition, _Port) ->
    Acc;
fold_all_objects([Bucket | RestBuckets], Acc, FoldObjectsFun, Partition, Port) ->
    KeyStore = keystore(Bucket, Partition, Port),
    FoldFun = fold_objects_fun(FoldObjectsFun, Bucket),
    case innostore:fold(FoldFun, Acc, KeyStore) of
        {error, Reason} ->
            {error, Reason};
        Acc1 ->
            fold_all_objects(RestBuckets, Acc1, FoldObjectsFun, Partition, Port)
    end.

%% @private
bucket_from_tablename(TableName) ->
    {match, [Name]} = re:run(TableName, "(.*)_\\d+", [{capture, all_but_first, binary}]),
    Name.

%% @private
format_status([]) -> ok;
format_status([{K,V}|T]) ->
    io:format("~p: ~p~n", [K,V]),
    format_status(T).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

standard_test_() ->
    Config = [
              {data_home_dir,            "./test/innodb-backend"},
              {log_group_home_dir,       "./test/innodb-backend"},
              {buffer_pool_size,         2147483648}
             ],
    {spawn,
     [
      {setup,
       fun() -> setup(Config) end,
       fun cleanup/1,
       fun(X) ->
               [basic_store_and_fetch(X),
                fold_buckets(X),
                fold_keys(X),
                delete_object(X),
                fold_objects(X),
                empty_check(X)
               ]
       end
      }]}.

basic_store_and_fetch(State) ->
    {"basic store and fetch test",
     fun() ->
             [
              ?_assertMatch({ok, _},
                            ?MODULE:put(<<"b1">>, <<"k1">>, [], <<"v1">>, State)),
              ?_assertMatch({ok, _},
                            ?MODULE:put(<<"b2">>, <<"k2">>, [], <<"v2">>, State)),
              ?_assertMatch({ok,<<"v2">>, _},
                            ?MODULE:get(<<"b2">>, <<"k2">>, State)),
              ?_assertMatch({error, not_found, _},
                            ?MODULE:get(<<"b1">>, <<"k3">>, State))
             ]
     end
    }.

fold_buckets(State) ->
    {"bucket folding test",
     fun() ->
             FoldBucketsFun =
                 fun(Bucket, Acc) ->
                         [Bucket | Acc]
                 end,

             ?_assertEqual([<<"b1">>, <<"b2">>],
                           begin
                               {ok, Buckets1} =
                                   ?MODULE:fold_buckets(FoldBucketsFun,
                                                        [],
                                                        [],
                                                        State),
                               lists:sort(Buckets1)
                           end)
     end
    }.

fold_keys(State) ->
    {"key folding test",
     fun() ->
             FoldKeysFun =
                 fun(Bucket, Key, Acc) ->
                         [{Bucket, Key} | Acc]
                 end,
             FoldKeysFun1 =
                 fun(_Bucket, Key, Acc) ->
                         [Key | Acc]
                 end,
             FoldKeysFun2 =
                 fun(Bucket, Key, Acc) ->
                         case Bucket =:= <<"b1">> of
                             true ->
                                 [Key | Acc];
                             false ->
                                 Acc
                         end
                 end,
             FoldKeysFun3 =
                 fun(Bucket, Key, Acc) ->
                         case Bucket =:= <<"b1">> of
                             true ->
                                 Acc;
                             false ->
                                 [Key | Acc]
                         end
                 end,
             [
              ?_assertEqual([{<<"b1">>, <<"k1">>}, {<<"b2">>, <<"k2">>}],
                            begin
                                {ok, Keys1} =
                                    ?MODULE:fold_keys(FoldKeysFun,
                                                      [],
                                                      [],
                                                      State),
                                lists:sort(Keys1)
                            end),
              ?_assertEqual({ok, [<<"k1">>]},
                            ?MODULE:fold_keys(FoldKeysFun1,
                                              [],
                                              [{bucket, <<"b1">>}],
                                              State)),
              ?_assertEqual([<<"k2">>],
                            ?MODULE:fold_keys(FoldKeysFun1,
                                              [],
                                              [{bucket, <<"b2">>}],
                                              State)),
              ?_assertEqual({ok, [<<"k1">>]},
                            ?MODULE:fold_keys(FoldKeysFun2, [], [], State)),
              ?_assertEqual({ok, [<<"k1">>]},
                            ?MODULE:fold_keys(FoldKeysFun2,
                                              [],
                                              [{bucket, <<"b1">>}],
                                              State)),
              ?_assertEqual({ok, [<<"k2">>]},
                            ?MODULE:fold_keys(FoldKeysFun3, [], [], State)),
              ?_assertEqual({ok, []},
                            ?MODULE:fold_keys(FoldKeysFun3,
                                              [],
                                              [{bucket, <<"b1">>}],
                                              State))
             ]
     end
    }.

delete_object(State) ->
    {"object deletion test",
     fun() ->
             [
              ?_assertMatch({ok, _}, ?MODULE:delete(<<"b2">>, <<"k2">>, State)),
              ?_assertMatch({error, not_found, _},
                            ?MODULE:get(<<"b2">>, <<"k2">>, State))
             ]
     end
    }.

fold_objects(State) ->
    {"object folding test",
     fun() ->
             FoldKeysFun =
                 fun(Bucket, Key, Acc) ->
                         [{Bucket, Key} | Acc]
                 end,
             FoldObjectsFun =
                 fun(Bucket, Key, Value, Acc) ->
                         [{{Bucket, Key}, Value} | Acc]
                 end,
             [
              ?_assertEqual([{<<"b1">>, <<"k1">>}],
                            begin
                                {ok, Keys} =
                                    ?MODULE:fold_keys(FoldKeysFun,
                                                      [],
                                                      [],
                                                      State),
                                lists:sort(Keys)
                            end),

              ?_assertEqual([{{<<"b1">>,<<"k1">>}, <<"v1">>}],
                            begin
                                {ok, Objects1} =
                                    ?MODULE:fold_objects(FoldObjectsFun,
                                                         [],
                                                         [],
                                                         State),
                                lists:sort(Objects1)
                            end),
              ?_assertMatch({ok, _},
                            ?MODULE:put(<<"b3">>, <<"k3">>, [], <<"v3">>, State)),
              ?_assertEqual([{{<<"b1">>,<<"k1">>},<<"v1">>},
                             {{<<"b3">>,<<"k3">>},<<"v3">>}],
                            begin
                                {ok, Objects} =
                                    ?MODULE:fold_objects(FoldObjectsFun,
                                                         [],
                                                         [],
                                                         State),
                                lists:sort(Objects)
                            end)
             ]
     end
    }.

empty_check(State) ->
    {"is_empty test",
     fun() ->
             [
              ?_assertEqual(false, ?MODULE:is_empty(State)),
              ?_assertMatch({ok, _}, ?MODULE:delete(<<"b1">>,<<"k1">>, State)),
              ?_assertMatch({ok, _}, ?MODULE:delete(<<"b3">>,<<"k3">>, State)),
              ?_assertEqual(true, ?MODULE:is_empty(State))
             ]
     end
    }.

setup(Config) ->
    %% Start the backend
    {ok, S} = ?MODULE:start(42, Config),
    S.

cleanup(S) ->
    ok = ?MODULE:stop(S),
    {ok, Port} = innostore:connect(),
    [ok = innostore:drop_keystore(T, Port) || T <- innostore:list_keystores(Port)],
    innostore:disconnect(Port),
    ok.

-endif.
