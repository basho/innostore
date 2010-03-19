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
         list_bucket/2,
         fold/3,
         is_empty/1,
         drop/1]).


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
    innostore:disconnect(State#state.port).

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
    list_keys(true, list_buckets(State), [], State).

list_bucket(State, Bucket) ->
    %% List all keys in a bucket
    Name = <<Bucket/binary, (State#state.partition_str)/binary>>,
    list_keys(false, [Name], [], State).


%% ===================================================================
%% Internal functions
%% ===================================================================

key_entry(undefined,Key) -> Key;
key_entry(Bucket,Key) -> {Bucket,Key}.    

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

list_keys(_IncludeBucket, [], Acc, _State) ->
    Acc;
list_keys(IncludeBucket, [Name | Rest], Acc, State) ->
    Bucket = case IncludeBucket of
        true -> bucket_from_tablename(Name);
        false -> undefined
    end,
    {ok, Store} = innostore:open_keystore(Name, State#state.port),
    case innostore:fold_keys(fun(K, Acc1) -> [key_entry(Bucket, K) | Acc1] end,
                             Acc, Store) of
        {error, Reason} ->
            {error, Reason};
        Acc2 ->
            list_keys(IncludeBucket, Rest, Acc2, State)
    end.

fold(State, Fun0, Acc0) ->
    lists:flatten(
      [innostore:fold(
         fun(K,V,A) -> 
                 Fun0({bucket_from_tablename(B),K},V,A) end,
         Acc0, keystore(bucket_from_tablename(B), State)) || 
          B <- list_buckets(State)]).

is_empty(State) ->    
    lists:all(fun(I) -> I end,
              [innostore:is_keystore_empty(B,
                                           State#state.port) || 
                  B <- list_buckets(State)]).

drop(State) ->
    KSes = list_buckets(State),
    [innostore:drop_keystore(K, State#state.port) || K <- KSes],
    ok.

bucket_from_tablename(TableName) ->
    {match, [Name]} = re:run(TableName, "(.*)_\\d+", [{capture, all_but_first, binary}]),
    Name.



%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-define(TEST_BUCKET, <<"test">>).
-define(OTHER_TEST_BUCKET, <<"othertest">>).

innostore_riak_test_() ->
    {spawn, [{"bucket_list",
               ?_test(
                  begin
                      reset(),
                      {ok, S1} = start(0, undefined),
                      {ok, S2} = start(1, undefined),
                      
                      ok = ?MODULE:put(S1, {?TEST_BUCKET, <<"key1">>}, <<"abcdef">>),
                      ok = ?MODULE:put(S2, {?TEST_BUCKET, <<"key2">>}, <<"dasdf">>),
                      ok = ?MODULE:put(S1, {?OTHER_TEST_BUCKET, <<"key1">>}, <<"123456">>),
                      
                      ["othertest_0", "test_0"] = lists:sort(list_buckets(S1)),
                      ["test_1"] = list_buckets(S2),
                      
                      [{?TEST_BUCKET, <<"key1">>}, {?OTHER_TEST_BUCKET, <<"key1">>}] = ?MODULE:list(S1),
                      [{?TEST_BUCKET, <<"key2">>}] = ?MODULE:list(S2)
                  end)},

              {"fold_test",
               ?_test(
                  begin
                      reset(),
                      {ok, S} = start(2, undefined),
                      ok = ?MODULE:put(S, {?TEST_BUCKET, <<"1">>}, <<"abcdef">>),
                      ok = ?MODULE:put(S, {?TEST_BUCKET, <<"2">>}, <<"foo">>),
                      ok = ?MODULE:put(S, {?TEST_BUCKET, <<"3">>}, <<"bar">>),
                      ok = ?MODULE:put(S, {?TEST_BUCKET, <<"4">>}, <<"baz">>),
                      [{{?TEST_BUCKET, <<"4">>}, <<"baz">>},
                       {{?TEST_BUCKET, <<"3">>}, <<"bar">>},
                       {{?TEST_BUCKET, <<"2">>}, <<"foo">>},
                       {{?TEST_BUCKET, <<"1">>}, <<"abcdef">>}]
                          = ?MODULE:fold(S, fun(K,V,A)->[{K,V}|A] end, [])
                  end)}
             ]}.

reset() ->
    {ok, Port} = innostore:connect(),
    [ok = innostore:drop_keystore(T, Port) || T <- innostore:list_keystores(Port)],
    ok.



-endif.
