-module(rmq_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

%%%===================================================================
%%% COMMON TEST CALLBACK FUNCTIONS
%%%===================================================================

suite() ->
    [{timetrap, {seconds, 10}}].

all() ->
    [{group, rmq, default, []}].

groups() ->
    [{rmq, [sequence], [start,
                        double_start,
                        connect,
                        send,
                        send_sync,
                        send_non_existent,
                        unexpected_messages,
                        reconnect,
                        connect_refused,
                        connect_bad_auth,
                        connect_consumer_non_existent_queue,
                        connect_producer_non_existent_exchange,
                        via_tuple_start,
                        via_tuple_send,
                        handle_cancel,
                        stop,
                        double_stop,
                        destroy_consumer,
                        destroy_producer]}].

init_per_suite(Config) ->
    application:set_env(log, level, debug),
    Config.

end_per_suite(_) ->
    ok.

init_per_group(_, Config) ->
    Config.

end_per_group(_, _) ->
    ok.

%%%===================================================================
%%% Tests
%%%===================================================================

start(_) ->
    disable_lager(),
    ?assertEqual(ok, log:start()),
    ?assertEqual(ok, rmq:start()).

double_start(_) ->
    ?assertEqual(ok, rmq:start()).

connect(_) ->
    URI = rmq_uri(),
    ProducerSpec = rmq:child_spec(URI, producer, config(producer)),
    ConsumerSpec = rmq:child_spec(URI, consumer, config(consumer)),
    ?assertMatch({ok, _}, supervisor:start_child(rmq_sup, ProducerSpec)),
    ?assertMatch({ok, _}, supervisor:start_child(rmq_sup, ConsumerSpec)),
    ct:sleep({seconds, 1}).

send(_) ->
    Body = term_to_binary(self()),
    ?assertEqual(ok, rmq:send(producer, <<"test">>, <<"test">>, Body,
                              #{content_type => <<"text/plain">>})),
    receive
        {reply, RoutingKey} ->
            ?assertEqual(<<"test">>, RoutingKey)
    after
        5000 ->
            erlang:error(timeout)
    end.

send_sync(_) ->
    Body = term_to_binary(self()),
    ?assertEqual(ok, rmq:send(producer, <<"test">>, <<"test">>, Body,
                              #{content_type => <<"text/plain">>}), sync),
    receive
        {reply, RoutingKey} ->
            ?assertEqual(<<"test">>, RoutingKey)
    after
        5000 ->
            erlang:error(timeout)
    end.

send_non_existent(_) ->
    ?assertEqual({error, noproc},
                 rmq:send(non_existent, <<"test">>, <<"test">>, <<"test">>, #{})).

unexpected_messages(_) ->
    [{_, Pid, _, _}|_] = supervisor:which_children(rmq_sup),
    ?assertEqual(ok, gen_server:cast(Pid, unexpected_cast_test)),
    ?assertExit({timeout, _}, gen_server:call(Pid, unexpected_call_test, 100)),
    erlang:send(Pid, unexpected_info_test).

reconnect(_) ->
    [{_, Pid, _, _}|_] = supervisor:which_children(rmq_sup),
    {monitors, [{process, Channel}]} = process_info(Pid, monitors),
    exit(Channel, kill).

connect_refused(_) ->
    URI = uri_string:parse("amqp://127.0.0.1:100"),
    Spec = rmq:child_spec(URI, refused, config(consumer)),
    ?assertMatch({error, {{{badmatch, {error, connection_failure}}, _}, _}},
                 supervisor:start_child(rmq_sup, Spec)).

connect_bad_auth(_) ->
    URI = rmq_uri(),
    URI1 = URI#{userinfo => <<>>},
    Spec = rmq:child_spec(URI1, bad_auth, config(consumer)),
    ?assertMatch({error, {{{badmatch, {error, connection_failure}}, _}, _}},
                 supervisor:start_child(rmq_sup, Spec)).

connect_consumer_non_existent_queue(_) ->
    URI = rmq_uri(),
    Config = (maps:without([create], config(consumer)))#{queue => <<"other_queue">>},
    Spec = rmq:child_spec(URI, non_existent_queue, Config),
    ?assertMatch({error, {{{badmatch, {error, cant_subscribe}}, _}, _}},
                 supervisor:start_child(rmq_sup, Spec)).

connect_producer_non_existent_exchange(_) ->
    URI = rmq_uri(),
    Config = (maps:without([create], config(producer)))#{exchange => <<"other_exchange">>},
    Spec = rmq:child_spec(URI, non_existent_exchange, Config),
    ?assertMatch({error, {{{badmatch, {error, cant_check_exchange}}, _}, _}},
                 supervisor:start_child(rmq_sup, Spec)).

via_tuple_start(_) ->
    {ok, _} = application:ensure_all_started(registrar),
    ok = registrar_local:start(),
    URI = rmq_uri(),
    ConsumerSpec = rmq:child_spec(URI,
                                  {via, registrar_local, {custom, consumer}},
                                  config(consumer)),
    ProducerSpec = rmq:child_spec(URI,
                                  {via, registrar_local, {custom, producer}},
                                  config(producer)),
    ?assertMatch({ok, _}, supervisor:start_child(rmq_sup, ConsumerSpec)),
    ?assertMatch({ok, _}, supervisor:start_child(rmq_sup, ProducerSpec)),
    ct:sleep({seconds, 1}).

via_tuple_send(_) ->
    Body = term_to_binary(self()),
    ?assertEqual(ok, rmq:send({via, registrar_local, {custom, producer}},
                              <<"test">>,
                              <<"test">>,
                              Body,
                              #{content_type => <<"text/plain">>})),
    receive
        {reply, RoutingKey} ->
            ?assertEqual(<<"test">>, RoutingKey)
    after
        5000 ->
            erlang:error(timeout)
    end.

handle_cancel(_) ->
    URI = rmq_uri(),
    ProducerSpec = rmq:child_spec(URI, producer2, config(producer2)),
    ConsumerSpec = rmq:child_spec(URI, consumer2, config(consumer2)),
    {ok, _} = supervisor:start_child(rmq_sup, ProducerSpec),
    {ok, ConsumerPid} = supervisor:start_child(rmq_sup, ConsumerSpec),
    Monitor = erlang:monitor(process, ConsumerPid),
    ct:sleep({seconds, 1}),

    ManagementURI = uri_string:parse("http://cleep:cleep@127.0.0.1:15672"),
    #{queue := Queue, create := #{exchange := BoundedExchange}} = config(consumer2),
    ok = rmq_management:destroy_consumer(ManagementURI, <<"/">>, Queue, BoundedExchange, timer:seconds(5)),
    ct:sleep({seconds, 1}),
    receive
        {'DOWN', Monitor, process, ConsumerPid, cancelled} -> % `cancelled` == basic.cancel is met
            ok
    after
        5000 ->
            throw(timeout)
    end.

stop(_) ->
    ?assertEqual(ok, rmq:stop()).

double_stop(_) ->
    ?assertEqual(ok, rmq:stop()).

destroy_consumer(_) ->
    ManagementURI = uri_string:parse("http://cleep:cleep@127.0.0.1:15672"),
    #{queue := Queue, create := #{exchange := BoundedExchange}} = config(consumer),
    ok = rmq_management:destroy_consumer(ManagementURI, <<"/">>, Queue, BoundedExchange, timer:seconds(5)).

destroy_producer(_) ->
    ManagementURI = uri_string:parse("http://cleep:cleep@127.0.0.1:15672"),
    Exchange = maps:get(exchange, config(producer)),
    ok = rmq_management:destroy_producer(ManagementURI, <<"/">>, Exchange, timer:seconds(5)).


%%%===================================================================
%%% RMQ handler
%%%===================================================================
handle_msg(RoutingKey, Body, ExtraArgs) ->
    [] = ExtraArgs,
    Sender = binary_to_term(Body),
    Sender ! {reply, RoutingKey},
    ack.

%%%===================================================================
%%% Misc
%%%===================================================================
config(consumer) ->
    #{queue => <<"test">>,
      handler => ?MODULE,
      ack => true,
      type => consumer,
      create => #{exchange => <<"test">>,
                  queue_x_queue_type => quorum}};
config(producer) ->
    #{type => producer,
      exchange => <<"test">>,
      create => #{}};

config(consumer2) ->
    #{queue => <<"test2">>,
      handler => ?MODULE,
      ack => true,
      type => consumer,
      create => #{exchange => <<"test2">>}};
config(producer2) ->
    #{type => producer,
      exchange => <<"test2">>,
      create => #{}}.

rmq_uri() ->
    uri_string:parse("amqp://rmq:rmq@127.0.0.1:5672").

disable_lager() ->
    application:set_env(lager, crash_log, false),
    application:set_env(lager, error_logger_redirect, false),
    application:set_env(lager, handlers, []).
