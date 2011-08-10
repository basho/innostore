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
         put/4,
         delete/3,
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
-define(CAPABILITIES, []).

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
            {error, notfound, State};
        {ok, Value} ->
            {ok, Value, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Insert an object into the innostore backend
-spec put(bucket(), key(), binary(), state()) ->
                 {ok, state()} |
                 {error, term(), state()}.
put(Bucket, Key, Value, #state{partition_str=Partition,
                               port=Port}=State) ->
    KeyStore = keystore(Bucket, Partition, Port),
    case innostore:put(Key, Value, KeyStore) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Delete an object from the innostore backend
-spec delete(bucket(), key(), state()) ->
                    {ok, state()} |
                    {error, term(), state()}.
delete(Bucket, Key, #state{partition_str=Partition,
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
                   state()) -> {ok, any()} | {error, term()}.
fold_buckets(FoldBucketsFun, Acc, _Opts, #state{partition_str=Partition,
                                                port=Port}) ->
    FoldFun = fold_buckets_fun(FoldBucketsFun),
    Buckets = list_buckets(Partition, Port),
    lists:foldl(FoldFun, Acc, Buckets).

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, term()} | {error, term()}.
fold_keys(FoldKeysFun, Acc, Opts, #state{partition_str=Partition,
                                         port=Port}) ->
    Bucket =  proplists:get_value(bucket, Opts),
    case Bucket of
        undefined ->
            Buckets = list_buckets(Partition, Port),
            %% Fold over all keys in all buckets
            fold_all_keys(Buckets, Acc, FoldKeysFun, Partition, Port);
        _ ->
            FoldFun = fold_keys_fun(FoldKeysFun, Bucket),
            KeyStore = keystore(Bucket, Partition, Port),
            innostore:fold_keys(FoldFun, Acc, KeyStore)
    end.

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {error, term()}.
fold_objects(FoldObjectsFun, Acc, Opts, #state{partition_str=Partition,
                                               port=Port}) ->
    Bucket =  proplists:get_value(bucket, Opts),
    case Bucket of
        undefined ->
            Buckets = list_buckets(Partition, Port),
            %% Fold over all objects in all buckets
            fold_all_objects(Buckets, Acc, FoldObjectsFun, Partition, Port);
        _ ->
            FoldFun = fold_objects_fun(FoldObjectsFun, Bucket),
            KeyStore = keystore(Bucket, Partition, Port),
            innostore:fold(FoldFun, Acc, KeyStore)
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
            erlang:put({innostore, KeyStore}, KeyStore),
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
%% Return a function to fold over keys on this backend
fold_keys_fun(FoldKeysFun, Bucket) ->
    fun(Key, Acc) ->
            FoldKeysFun(Bucket, Key, Acc)
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

-define(TEST_BUCKET, <<"test">>).
-define(OTHER_TEST_BUCKET, <<"othertest">>).

innostore_riak_test_() ->
    {spawn, [{"fold_buckets_test",
              ?_test(
                 begin
                     reset(),
                     {ok, S1} = start(0, undefined),
                     {ok, S2} = start(1, undefined),

                     {ok, S1} = ?MODULE:put(?TEST_BUCKET, <<"p0key1">>, <<"abcdef">>, S1),
                     {ok, S1} = ?MODULE:put(?TEST_BUCKET, <<"p0key2">>, <<"abcdef">>, S1),
                     {ok, S2} = ?MODULE:put(?TEST_BUCKET, <<"p1key2">>, <<"dasdf">>, S2),
                     {ok, S1} = ?MODULE:put(?OTHER_TEST_BUCKET, <<"p0key3">>, <<"123456">>, S1),

                     FoldBucketsFun =
                         fun(Bucket, Acc) ->
                                 [Bucket | Acc]
                         end,
                     FoldKeysFun =
                         fun(_Bucket, Key, Acc) ->
                                 [Key | Acc]
                         end,

                     ?assertEqual([?OTHER_TEST_BUCKET, ?TEST_BUCKET],
                                  lists:sort(fold_buckets(FoldBucketsFun, [], [], S1))),

                     ?assertEqual([<<"p0key1">>,<<"p0key2">>],
                                  lists:sort(fold_keys(FoldKeysFun, [], [{bucket, ?TEST_BUCKET}], S1))),

                     ?assertEqual([<<"p1key2">>],
                                  lists:sort(fold_keys(FoldKeysFun, [], [{bucket, ?TEST_BUCKET}], S2))),

                     ?assertEqual([<<"p0key3">>],
                                  lists:sort(fold_keys(FoldKeysFun, [], [{bucket, ?OTHER_TEST_BUCKET}], S1))),

                     FindKeyFun = fun(<<"p0key1">>) -> true; (_) -> false end,
                     FoldKeysFun1 =
                         fun(_Bucket, Key, Acc) ->
                                 case FindKeyFun(Key) of
                                     true ->
                                         [Key | Acc];
                                     false ->
                                         Acc
                                 end
                         end,

                     ?assertEqual([<<"p0key1">>],
                                  lists:sort(fold_keys(FoldKeysFun1, [], [{bucket, ?TEST_BUCKET}], S1))),

                     NotKeyFun = fun(<<"p0key1">>) -> false; (_) -> true end,
                     FoldKeysFun2 =
                         fun(_Bucket, Key, Acc) ->
                                 case NotKeyFun(Key) of
                                     true ->
                                         [Key | Acc];
                                     false ->
                                         Acc
                                 end
                         end,

                     ?assertEqual([<<"p0key2">>],
                                  lists:sort(fold_keys(FoldKeysFun2, [], [{bucket, ?TEST_BUCKET}], S1))),

                     FoldKeysFun3 =
                         fun(Bucket, Key, Acc) ->
                                 [{Bucket, Key} | Acc]
                         end,
                     ?assertEqual([{?OTHER_TEST_BUCKET, <<"p0key3">>},
                                   {?TEST_BUCKET, <<"p0key1">>},
                                   {?TEST_BUCKET, <<"p0key2">>}],
                                  lists:sort(fold_keys(FoldKeysFun3, [], [], S1))),
                     ?assertEqual([{?TEST_BUCKET, <<"p1key2">>}],
                                  lists:sort(fold_keys(FoldKeysFun3, [], [], S2)))
                 end)},

             {"fold_keys_test",
              ?_test(
                 begin
                     reset(),
                     {ok, S1} = start(5, undefined),
                     {ok, S1} = ?MODULE:put(?TEST_BUCKET, <<"abc">>, <<"123">>, S1),
                     {ok, S1} = ?MODULE:put(?TEST_BUCKET, <<"def">>, <<"456">>, S1),
                     {ok, S1} = ?MODULE:put(?TEST_BUCKET, <<"ghi">>, <<"789">>, S1),
                     FoldKeysFun =
                         fun(Bucket, Key, Acc) ->
                                 [{Bucket, Key} | Acc]
                         end,
                     [{?TEST_BUCKET, <<"ghi">>},
                      {?TEST_BUCKET, <<"def">>},
                      {?TEST_BUCKET, <<"abc">>}] =
                         ?MODULE:fold_keys(FoldKeysFun, [], [{bucket, ?TEST_BUCKET}], S1)
                 end)},

             {"fold_objects_test",
              ?_test(
                 begin
                     reset(),
                     {ok, S} = start(2, undefined),
                     {ok, S} = ?MODULE:put(?TEST_BUCKET, <<"1">>, <<"abcdef">>, S),
                     {ok, S} = ?MODULE:put(?TEST_BUCKET, <<"2">>, <<"foo">>, S),
                     {ok, S} = ?MODULE:put(?TEST_BUCKET, <<"3">>, <<"bar">>, S),
                     {ok, S} = ?MODULE:put(?TEST_BUCKET, <<"4">>, <<"baz">>, S),
                     FoldObjectsFun =
                         fun(Bucket, Key, Value, Acc) ->
                                 [{{Bucket, Key}, Value} | Acc]
                         end,
                     [{{?TEST_BUCKET, <<"4">>}, <<"baz">>},
                      {{?TEST_BUCKET, <<"3">>}, <<"bar">>},
                      {{?TEST_BUCKET, <<"2">>}, <<"foo">>},
                      {{?TEST_BUCKET, <<"1">>}, <<"abcdef">>}]
                         = ?MODULE:fold_objects(FoldObjectsFun, [], [{bucket, ?TEST_BUCKET}], S)
                 end)},
             {"status test",
              ?_test(
                 begin
                     reset(),
                     {ok, S} = start(2, undefined),
                     ?assertEqual(ok, status(S))
                 end)}
            ]}.

reset() ->
    {ok, Port} = innostore:connect(),
    [ok = innostore:drop_keystore(T, Port) || T <- innostore:list_keystores(Port)],
    ok.

-endif.
