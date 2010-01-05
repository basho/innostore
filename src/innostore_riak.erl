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
-module(innostore_riak).

-author('Dave Smith <dizzyd@basho.com>').

%% Public API for riak
-export([start/2,
         stop/1,
         get/2,
         put/3,
         delete/2,
         list/1,
         list_bucket/2]).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, { partition_str,
                 port }).

%% ===================================================================
%% Public API
%% ===================================================================

start(Partition, _Config) ->
    case innostore:connect() of
        {ok, Port} ->
            PartitionStr = <<"_", (list_to_binary(integer_to_list(Partition)))/binary>>,
            {ok, #state { partition_str = PartitionStr, port = Port }};
        {error, Reason} ->
            {error, Reason}
    end.

stop(State) ->
    innostore:close(State#state.port).

get(State, {Bucket, Key}) ->
    case innostore:get(Key, keystore(Bucket, State)) of
        {ok, not_found} ->
            {error, notfound};
        {ok, Value} ->
            {ok, Value};
        {error, Reason} ->
            {error, Reason}
    end.

put(State, {Bucket, Key}, Value) ->
    innostore:put(Key, Value, keystore(Bucket, State)).

delete(State, {Bucket, Key}) ->
    innostore:delete(Key, keystore(Bucket, State)).

list(State) ->
    %% List all keys in all buckets
    list_keys(list_buckets(State), [], State).

list_bucket(State, Bucket) ->
    %% List all keys in a bucket
    Name = <<Bucket/binary, (State#state.partition_str)/binary>>,
    list_keys([Name], [], State).


%% ===================================================================
%% Internal functions
%% ===================================================================

keystore(Bucket, State) ->
    Name = <<Bucket/binary, (State#state.partition_str)/binary>>,
    case erlang:get({innostore, Name}) of
        undefined ->
            {ok, Store} = innostore:open_keystore(Name, State#state.port),
            erlang:put({innostore, Name}, Store),
            Store;
        Store ->
            Store
    end.

list_buckets(State) ->
    Suffix = binary_to_list(State#state.partition_str),
    [T || T <- innostore:list_keystores(State#state.port),
         lists:suffix(Suffix, T) == true].

list_keys([], Acc, _State) ->
    Acc;
list_keys([Name | Rest], Acc, State) ->
    Bucket = bucket_from_tablename(Name),
    {ok, Store} = innostore:open_keystore(Name, State#state.port),
    case innostore:fold_keys(fun(K, Acc1) -> [{Bucket, K} | Acc1] end, Acc, Store) of
        {error, Reason} ->
            {error, Reason};
        Acc2 ->
            list_keys(Rest, Acc2, State)
    end.


bucket_from_tablename(TableName) ->
    {match, [Name]} = re:run(TableName, "(.*)_\\d+", [{capture, all_but_first, binary}]),
    Name.



%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-define(TEST_BUCKET, <<"test">>).
-define(OTHER_TEST_BUCKET, <<"othertest">>).

reset() ->
    {ok, Port} = innostore:connect(),
    [ok = innostore:drop_keystore(T, Port) || T <- innostore:list_keystores(Port)],
    ok.

bucket_list_test() ->
    reset(),
    {ok, S1} = start(0, undefined),
    {ok, S2} = start(1, undefined),

    ok = ?MODULE:put(S1, {?TEST_BUCKET, <<"key1">>}, <<"abcdef">>),
    ok = ?MODULE:put(S2, {?TEST_BUCKET, <<"key2">>}, <<"dasdf">>),
    ok = ?MODULE:put(S1, {?OTHER_TEST_BUCKET, <<"key1">>}, <<"123456">>),

    ["othertest_0", "test_0"] = lists:sort(list_buckets(S1)),
    ["test_1"] = list_buckets(S2),

    [{?TEST_BUCKET, <<"key1">>}, {?OTHER_TEST_BUCKET, <<"key1">>}] = ?MODULE:list(S1),
    [{?TEST_BUCKET, <<"key2">>}] = ?MODULE:list(S2).


-endif.
