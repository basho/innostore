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

%% Public API for riak
-export([start/2,
         stop/1,
         get/2,
         put/3,
         delete/2,
         list/1,
         fold_bucket_keys/3,
         list_bucket/2,
         fold/3,
         is_empty/1,
         drop/1,
         callback/3,
         status/0, status/1]).


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
    list_keys(true, list_buckets(State), [], undefined, State).

list_bucket(State, '_') -> %% List bucket names
    [bucket_from_tablename(TN) || TN <- list_buckets(State)];
list_bucket(State, {filter, Bucket, Fun}) ->
    %% Filter keys in a bucket
    Name = <<Bucket/binary, (State#state.partition_str)/binary>>,
    list_keys(false, [Name], [], Fun, State);
list_bucket(State, Bucket) ->
    %% List all keys in a bucket
    Name = <<Bucket/binary, (State#state.partition_str)/binary>>,
    list_keys(false, [Name], [], undefined, State).

fold_bucket_keys(State, Bucket0, Visitor) when is_function(Visitor) ->
    Bucket = <<Bucket0/binary, (State#state.partition_str)/binary>>,
    {ok, Store} = innostore:open_keystore(Bucket, State#state.port),
    case innostore:fold_keys(Visitor, [], Store) of
        {error, Reason} ->
            {error, Reason};
        Acc ->
            Acc
    end.

fold(State, Fun0, Acc0) ->
    fold_buckets(list_buckets(State), State, Fun0, Acc0).

is_empty(State) ->
    lists:all(fun(I) -> I end,
              [innostore:is_keystore_empty(B, State#state.port) ||
                  B <- list_buckets(State)]).

drop(State) ->
    KSes = list_buckets(State),
    [innostore:drop_keystore(K, State#state.port) || K <- KSes],
    ok.

status() ->
    status([]).

status(Names) ->
    {ok, Port} = innostore:connect(),
    try
        Status = case Names of
                     [] ->
                         innostore:status(Port);
                     _ ->
                         [begin
                              A = to_atom(N), 
                              {A, innostore:status(A, Port)}
                          end || N <- Names]
                 end,
        format_status(Status)
    after
        innostore:disconnect(Port)
    end,
    ok.

%% Ignore callbacks we do not know about - may be in multi backend
callback(_State, _Ref, _Msg) ->
    ok.


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

list_keys(_IncludeBucket, [], Acc, _Pred, _State) ->
    Acc;
list_keys(IncludeBucket, [Name | Rest], Acc, Pred, State) ->
    Bucket = case IncludeBucket of
        true -> bucket_from_tablename(Name);
        false -> undefined
    end,
    case Pred of
        undefined ->
            Visitor = fun(K, Acc1) -> [key_entry(Bucket, K) | Acc1] end;
        
        Pred when is_function(Pred)  ->
            Visitor = fun(K, Acc1) ->
                              Entry = key_entry(Bucket, K),
                              case Pred(Entry) of 
                                  true ->
                                      [Entry | Acc1];
                                  false ->
                                      Acc1
                              end
                      end
    end,
    {ok, Store} = innostore:open_keystore(Name, State#state.port),
    case innostore:fold_keys(Visitor, Acc, Store) of
        {error, Reason} ->
            {error, Reason};
        Acc2 ->
            list_keys(IncludeBucket, Rest, Acc2, Pred, State)
    end.

fold_buckets([], _State, _Fun, Acc0) ->
    Acc0;
fold_buckets([B | Rest], State, Fun, Acc0) ->
    Bucket = bucket_from_tablename(B),
    F = fun(K, V, A) ->
                Fun({Bucket, K}, V, A)
        end,
    Acc = innostore:fold(F, Acc0, keystore(Bucket, State)),
    fold_buckets(Rest, State, Fun, Acc).



bucket_from_tablename(TableName) ->
    {match, [Name]} = re:run(TableName, "(.*)_\\d+", [{capture, all_but_first, binary}]),
    Name.

to_atom(A) when is_atom(A) ->
    A;
to_atom(S) when is_list(S) ->
    list_to_existing_atom(S);
to_atom(B) when is_binary(B) ->
    binary_to_existing_atom(B, utf8).

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
    {spawn, [{"bucket_list",
               ?_test(
                  begin
                      reset(),
                      {ok, S1} = start(0, undefined),
                      {ok, S2} = start(1, undefined),
                      
                      ok = ?MODULE:put(S1, {?TEST_BUCKET, <<"p0key1">>}, <<"abcdef">>),
                      ok = ?MODULE:put(S1, {?TEST_BUCKET, <<"p0key2">>}, <<"abcdef">>),
                      ok = ?MODULE:put(S2, {?TEST_BUCKET, <<"p1key2">>}, <<"dasdf">>),
                      ok = ?MODULE:put(S1, {?OTHER_TEST_BUCKET, <<"p0key3">>}, <<"123456">>),
                      
                      ["othertest_0", "test_0"] = lists:sort(list_buckets(S1)),
                      ["test_1"] = list_buckets(S2),
                      
                      ?assertEqual([?OTHER_TEST_BUCKET, ?TEST_BUCKET], 
                                   lists:sort(list_bucket(S1, '_'))),

                      ?assertEqual([<<"p0key1">>,<<"p0key2">>],
                                   lists:sort(list_bucket(S1, ?TEST_BUCKET))),

                      ?assertEqual([<<"p0key3">>],
                                   lists:sort(list_bucket(S1, ?OTHER_TEST_BUCKET))),

                      FindKey1 = fun(<<"p0key1">>) -> true; (_) -> false end,
                      ?assertEqual([<<"p0key1">>], 
                                   lists:sort(list_bucket(S1, {filter, ?TEST_BUCKET, FindKey1}))),

                      NotKey1 = fun(<<"p0key1">>) -> false; (_) -> true end,
                      ?assertEqual([<<"p0key2">>], 
                                   lists:sort(list_bucket(S1, {filter, ?TEST_BUCKET, NotKey1}))),
                      

                      ?assertEqual([{?OTHER_TEST_BUCKET, <<"p0key3">>},
                                    {?TEST_BUCKET, <<"p0key1">>}, 
                                    {?TEST_BUCKET, <<"p0key2">>}],
                                   lists:sort(?MODULE:list(S1))),
                      ?assertEqual([{?TEST_BUCKET, <<"p1key2">>}], 
                                   ?MODULE:list(S2))
                  end)},

             {"fold_bucket_keys_test",
              ?_test(
                 begin
                     reset(),
                     {ok, S1} = start(5, undefined),
                     ok = ?MODULE:put(S1, {?TEST_BUCKET, <<"abc">>}, <<"123">>),
                     ok = ?MODULE:put(S1, {?TEST_BUCKET, <<"def">>}, <<"456">>),
                     ok = ?MODULE:put(S1, {?TEST_BUCKET, <<"ghi">>}, <<"789">>),
                     F = fun(Key, Accum) -> [{?TEST_BUCKET, Key}|Accum] end,
                     [{?TEST_BUCKET, <<"ghi">>},
                      {?TEST_BUCKET, <<"def">>},
                      {?TEST_BUCKET, <<"abc">>}] =
                         ?MODULE:fold_bucket_keys(S1, ?TEST_BUCKET, F)
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
                  end)},
             {"status test",
              ?_test(
                 begin
                     ?assertEqual(ok, status()),
                     ?assertEqual(ok, status([page_size])),
                     ?assertEqual(ok, status(["page_size"])),
                     ?assertEqual(ok, status([<<"page_size">>]))
                 end)}
             ]}.

reset() ->
    {ok, Port} = innostore:connect(),
    [ok = innostore:drop_keystore(T, Port) || T <- innostore:list_keystores(Port)],
    ok.



-endif.
