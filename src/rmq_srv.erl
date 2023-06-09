-module(rmq_srv).

-behaviour(gen_server).

%% API
-export([start_link/4]).
-export([send/6]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("kernel/include/logger.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-record(state, {name :: rmq:connection_name(),
                uri :: uri_string:uri_map(),
                config :: rmq:config(),
                reconnect_timeout :: millisecond() | undefined,
                channel :: pid() | undefined,
                connection :: pid() | undefined,
                monitor_ref :: reference() | undefined,
                extra_handler_args :: rmq:extra_handler_args()}).

-type state() :: #state{}.
-type props() :: #'P_basic'{}.
-type millisecond() :: pos_integer().

%%%===================================================================
%%% API
%%%===================================================================
-spec start_link(uri_string:uri_map(), rmq:connection_name(),
                 rmq:config(), rmq:extra_handler_args()
) -> {ok, pid()} | {error, term()} | ignore.
start_link(URI, ConnName, Config, ExtraArgs) ->
    FullConfig = prepare_config(Config),
    Name = case ConnName of
               {via, M, _} = ViaTuple when is_atom(M) ->
                   ViaTuple;
               N when is_atom(N) ->
                   {local, N}
           end,
    gen_server:start_link(Name, ?MODULE, {URI, ConnName, FullConfig, ExtraArgs}, []).

-spec send(rmq:connection_name(), rmq:exchange(), rmq:routing_key(),
           binary(), rmq:send_opts(), sync | async) -> ok | {error,  noproc | {sync_send, _}}.
send(ConnName, Exchange, RoutingKey, Data, Opts, Sync) ->
    Props = amqp_props(Opts),
    Msg = #amqp_msg{props = Props, payload = Data},
    Publish = #'basic.publish'{exchange = Exchange, routing_key = RoutingKey},
    case whereis_channel(ConnName) of
        {error, {notfound, ProcNameOrVia}} ->
            ?LOG_DEBUG("Failed to send to ~p/~s: no such process",
                       [ProcNameOrVia, Exchange]),
            {error, noproc};
        {ok, {ProcNameOrVia, Pid}} ->
            ?LOG_DEBUG("Sending to ~p/~s (~p):~n"
                       "** Msg =~n~s~n"
                       "** Publish =~n~s",
                       [ProcNameOrVia, Exchange, Pid, pp(Msg), pp(Publish)]),
            case Sync of
                sync ->
                    try amqp_channel:call(Pid, Publish, Msg) of
                        ok -> ok;
                        Other -> {error, {sync_send, Other}}
                    catch
                        E:R ->
                            {error, {sync_send, {E, R}}}
                    end;
                async ->
                    amqp_channel:cast(Pid, Publish, Msg)
            end
    end.


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
-spec init({uri_string:uri_map(), rmq:connection_name(),
            rmq:config(), rmq:extra_handler_args()}) -> {ok, state()}.
init({URI, ConnName, Config, ExtraArgs}) ->
    process_flag(trap_exit, true),
    State = #state{uri = URI, name = ConnName,
                   config = Config, extra_handler_args = ExtraArgs},
    {noreply, State1} = connect(State, false),
    {ok, State1}.

-spec handle_call(term(), {pid(), term()}, state()) -> {noreply, state()}.
handle_call(Request, {Pid, _}, State) ->
    ?LOG_WARNING("Unexpected call from ~p: ~p", [Pid, Request]),
    {noreply, State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(Request, State) ->
    ?LOG_WARNING("Unexpected cast: ~p", [Request]),
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info({#'basic.deliver'{delivery_tag = Tag, routing_key = RK},
             {_, _, Body}} = Message,
            State = #state{channel = Channel,
                           config = #{handler := Handler},
                           extra_handler_args = ExtraArgs}) ->
    try rmq_handler:handle_msg(Handler, RK, Body, ExtraArgs) of
        ack ->
            amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),
            ?LOG_DEBUG("RabbitMQ message acked:~n~s", [pp(Message)]);
        nack ->
            amqp_channel:cast(Channel, #'basic.nack'{delivery_tag = Tag}),
            ?LOG_DEBUG("RabbitMQ message nacked:~n~s", [pp(Message)])
    catch Class:Reason:Stacktrace ->
            ?LOG_ERROR("RabbitMQ handler ~p failed to process message:~n~s~n** ~s",
                       [Handler, pp(Message),
                        format_exception(2, Class, Reason, Stacktrace)]),
            amqp_channel:cast(Channel, #'basic.nack'{delivery_tag = Tag})
    end,
    {noreply, State};
handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};
handle_info(#'basic.cancel'{}, State) ->
    % [NGibaev 27.05.2021] There is no need to match consumer tag explicitly,
    % because this gen_server already monitoring bounded channel. Client-side reply
    % with 'basic.cancel_ok' message is obsolete too, as broker does not expect replies
    % for cancelled consumers (see https://www.rabbitmq.com/consumer-cancel.html for details)
    {stop, cancelled, State};
handle_info({'DOWN', MRef, process, Connection, Reason},
            #state{uri = URI, monitor_ref = MRef, connection = Connection} = State) ->
    ?LOG_ERROR("Connection to RabbitMQ at ~s:~B has failed: ~p",
               [maps:get(host, URI), maps:get(port, URI), Reason]),
    reconnect(State, true);
handle_info(connect, State) ->
    connect(State, true);
handle_info(Info, State) ->
    ?LOG_WARNING("Unexpected info: ~p", [Info]),
    {noreply, State}.

-spec terminate(term(), state()) -> any().
terminate(_Reason, State) ->
    case State#state.connection of
        undefined -> ok;
        Pid -> amqp_connection:close(Pid)
    end.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
-spec whereis_channel(rmq:connection_name()
) -> {ok, {ChannelName, pid()}} | {error, {notfound, ChannelNameRet}}
              when ChannelName :: atom() | {via, atom(), term()},
                   ChannelNameRet :: string() | {via, atom(), term()}.
whereis_channel(ConnName) when is_atom(ConnName) ->
    ProcName = amqp_proc(ConnName),
    try whereis(list_to_existing_atom(ProcName)) of
        Pid ->
            {ok, {ProcName, Pid}}
    catch _:_ ->
            {error, {notfound, ProcName}}
    end;
whereis_channel({via, M, _Name} = Via) when is_atom(M) ->
    case M:whereis_name(amqp_proc(Via)) of
        undefined ->
            {error, {notfound, Via}};
        Pid ->
            {ok, {Via, Pid}}
    end.


-spec connect(state(), IsRestartable :: boolean()) -> {noreply, state()}.
connect(#state{uri = #{host := Host, port := Port} = URI,
               name = ConnName,
               reconnect_timeout = _,
               extra_handler_args = _,
               config = #{type := WorkerType} = Config,
               _ = undefined} = State,
        IsRestartable) ->
    {User, Pass} = parse_userinfo(URI),
    VHost = parse_path(URI),
    AmqpParams = #amqp_params_network{username = User,
                                      password = Pass,
                                      virtual_host = VHost,
                                      host = to_string(Host),
                                      port = Port},
    try
        ?LOG_DEBUG("Connecting to ~s using AMQP parameters:~n~s",
                   [uri_string:normalize(URI), pp(AmqpParams)]),
        {ok, Connection} = amqp_connection:start(AmqpParams),
        ?LOG_DEBUG("Openning channel inside connection ~p", [Connection]),
        {ok, Channel} = amqp_connection:open_channel(Connection),
        ok = create(WorkerType, Channel, State),
        _ = case WorkerType of
                producer ->
                    Exchange = maps:get(exchange, Config),
                    CheckExchange = #'exchange.declare'{exchange = Exchange, passive = true},
                    #'exchange.declare_ok'{} = amqp_channel:call(Channel, CheckExchange);
                consumer ->
                    #{queue := Queue,
                      ack := Ack,
                      handler := _} = Config,
                    PrefetchCount = maps:get(prefetch_count, Config, 1),
                    #'basic.qos_ok'{} = amqp_channel:call(Channel,
                                                          #'basic.qos'{
                                                             prefetch_count = PrefetchCount}),
                    Sub = #'basic.consume'{queue = Queue, no_ack = not Ack},
                    ?LOG_DEBUG("Subscribing to channel ~p with subscription:~n~s",
                               [Channel, pp(Sub)]),
                    #'basic.consume_ok'{} = amqp_channel:subscribe(Channel, Sub, self())
            end,
        case ConnName of
            _ when is_atom(ConnName) ->
                register(list_to_atom(amqp_proc(ConnName)), Channel);
            {via, M, Name} ->
                M:register_name({amqp_chan, Name}, Channel)
        end,
        MRef = erlang:monitor(process, Connection),
        {noreply, State#state{reconnect_timeout = undefined,
                              channel = Channel,
                              connection = Connection,
                              monitor_ref = MRef}}
    catch
        _:{badmatch, {error, Reason}} ->
            ?LOG_ERROR("Failed to connect to RabbitMQ at ~s:~B: ~s",
                       [maps:get(host, URI), maps:get(port, URI),
                        format_connection_error(Reason)]),
            reconnect(State, IsRestartable);
        exit:{{shutdown, {server_initiated_close, 404, _}}, _}
          when WorkerType == consumer ->
            ?LOG_ERROR("Failed to subscribe to queue, provided queue does not exist"),
            {error, cant_subscribe};
        exit:{{shutdown, {server_initiated_close, 404, _}}, _}
          when WorkerType == producer ->
            ?LOG_ERROR(("Failed to check required exchange for producer, "
                        "provided exchange does not exist")),
            {error, cant_check_exchange};
        exit:{{shutdown, Reason}, _} ->
            ?LOG_ERROR("Failed to connect to RabbitMQ at ~s:~B: ~s",
                       [maps:get(host, URI), maps:get(port, URI),
                        format_connection_error(Reason)]),
            reconnect(State, IsRestartable)
    end.

-spec create(rmq:worker_type(), pid(), state()) -> ok.
create(producer,
       Channel,
       #state{config = #{exchange := Exchange,
                         create :=
                             #{exchange_type := ExchangeType,
                               exchange_durable := IsExchangeDur,
                               exchange_auto_delete := IsExchangeAutoDel,
                               exchange_nowait := IsExchangeNoWait,
                               exchange_arguments := ExchangeArgs}}}) ->
    ExDeclare = #'exchange.declare'{exchange = Exchange,
                                    type = format_exchange_type(ExchangeType),
                                    durable = IsExchangeDur,
                                    auto_delete = IsExchangeAutoDel,
                                    nowait = IsExchangeNoWait,
                                    arguments = ExchangeArgs},
    ?LOG_DEBUG("Declaring exchange for channel ~p:~n~s",
               [Channel, pp(ExDeclare)]),
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, ExDeclare),
    ok;
create(consumer,
       Channel,
       #state{config = #{queue := Queue,
                         create :=
                             #{exchange := Exchange,
                               queue_durable := IsQueueDurable,
                               queue_exclusive := IsQueueExclusive,
                               queue_auto_delete := IsQueueAutoDel,
                               queue_nowait := IsQueueNoWait,
                               queue_binding_key := BindingKey} = Create}}) ->
    Durability =
        case maps:find(queue_x_queue_type, Create) of
            {ok, quorum} ->
                true;
            {ok, classic} ->
                IsQueueDurable;
            error ->
                IsQueueDurable
        end,
    QueueArgs = get_queue_x_args(Create),
    QDeclare = #'queue.declare'{queue = Queue,
                                exclusive = IsQueueExclusive,
                                auto_delete = IsQueueAutoDel,
                                nowait = IsQueueNoWait,
                                durable = Durability,
                                arguments = QueueArgs},
    ?LOG_DEBUG("Declaring queue for channel ~p:~n~s",
               [Channel, pp(QDeclare)]),
    #'queue.declare_ok'{} = amqp_channel:call(Channel, QDeclare),
    Binding = #'queue.bind'{queue = Queue,
                            exchange = Exchange,
                            routing_key = BindingKey},
    ?LOG_DEBUG("Binding to channel ~p:~n~s",
               [Channel, pp(Binding)]),
    #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding),
    ok;
create(_, _, _) ->
    ok.

-spec reconnect(state(), IsRestartable :: boolean()
) -> {noreply, state()} | {error, connection_failure}.
reconnect(_State, false) ->
    {error, connection_failure};
reconnect(#state{uri = URI} = State, true) ->
    Timeout = case State#state.reconnect_timeout of
                  undefined -> rand:uniform(1000);
                  T -> min(T*2, timer:seconds(30))
              end,
    ?LOG_DEBUG("Reconnecting to RabbitMQ at ~s:~B in ~.2fs",
               [maps:get(host, URI), maps:get(port, URI), Timeout/1000]),
    erlang:send_after(Timeout, self(), connect),
    State1 = State#state{reconnect_timeout = Timeout,
                         channel = undefined,
                         connection = undefined,
                         monitor_ref = undefined},
    {noreply, State1}.

-spec prepare_config(rmq:config()) -> rmq:config().
prepare_config(#{type := producer,
                 create := CreateConfig} = Config) ->
    FullCreateConfig = maps:merge(rmq:default_create_config(producer),
                                  CreateConfig),
    Config#{create => FullCreateConfig};
prepare_config(#{type := consumer,
                 queue := Queue,
                 create := CreateConfig} = Config) ->
    FullCreateConfig = maps:merge(rmq:default_create_config(consumer, Queue),
                                  CreateConfig),
    maps:merge(rmq:default_config(), Config#{create => FullCreateConfig});
prepare_config(Config) ->
    Config.

-spec get_queue_x_args(rmq:consumer_create_config()
) -> [{binary(), long, pos_integer()}] |
     [{binary(), longstr, binary()}].
get_queue_x_args(Config) ->
    maps:fold(
      fun(queue_x_expires, Val, Acc) ->
              [{<<"x-expires">>, long, Val}|Acc];
         (queue_x_message_ttl, Val, Acc) ->
              [{<<"x-message-ttl">>, long, Val}|Acc];
         (queue_x_max_length, Val, Acc) ->
              [{<<"x-max-length">>, long, Val}|Acc];
         (queue_x_max_length_bytes, Val, Acc) ->
              [{<<"x-max-length-bytes">>, long, Val}|Acc];
         (queue_x_queue_type, Val, Acc) ->
              [{<<"x-queue-type">>, longstr, atom_to_binary(Val, utf8)}|Acc];
         (queue_x_quorum_initial_group_size, Val, Acc) ->
              [{<<"x-quorum-initial-group-size">>, long, Val}|Acc];
         (_, _, Acc) ->
              Acc
      end, [], Config).

-spec amqp_props(rmq:send_opts()) -> props().
amqp_props(#{content_type := T}) ->
    #'P_basic'{content_type = T};
amqp_props(_) ->
    #'P_basic'{}.

-spec amqp_proc (atom()) -> string();
                ({via, atom(), Name}) -> {amqp_chan, Name} when Name :: term().
amqp_proc(Name) when is_atom(Name) ->
    ?MODULE_STRING ++ "_" ++ atom_to_list(Name);
amqp_proc({via, _, Name}) ->
    {amqp_chan, Name}.

-spec parse_userinfo(uri_string:uri_map()) -> {binary(), binary()}.
parse_userinfo(#{userinfo := UserInfo}) ->
    case binary:split(iolist_to_binary(UserInfo), <<$:>>) of
        [User, Pass] -> {User, Pass};
        [User] -> {User, <<>>}
    end;
parse_userinfo(_) ->
    {<<>>, <<>>}.

-spec parse_path(uri_string:uri_map()) -> binary().
parse_path(#{path := Path}) ->
    list_to_binary([string:trim(Path, trailing, "/"), $/]);
parse_path(_) ->
    <<$/>>.

-spec to_string(iodata()) -> string().
to_string(Data) ->
    binary_to_list(iolist_to_binary(Data)).

-spec format_exchange_type({custom, atom()} | atom()) -> binary().
format_exchange_type({custom, Type}) ->
    format_exchange_type(Type);
format_exchange_type(Type) ->
    atom_to_binary(Type, utf8).

format_exception(Level, Class, Reason, Stacktrace) ->
    erl_error:format_exception(
      Level, Class, Reason, Stacktrace,
      fun(_M, _F, _A) -> false end,
      fun(Term, I) ->
              io_lib:print(Term, I, 80, -1)
      end).

-spec format_connection_error(term()) -> iolist().
format_connection_error(Reason) when is_atom(Reason) ->
    case inet:format_error(Reason) of
        "unknown POSIX error" ->
            atom_to_list(Reason);
        ReasonS ->
            ReasonS
    end;
format_connection_error(Reason) ->
    pp(Reason).

-spec pp(any()) -> iolist().
pp(Term) ->
    io_lib_pretty:print(Term, fun pp/2).

-spec pp(atom(), non_neg_integer()) -> [atom()] | no.
pp(amqp_params_network, _) -> record_info(fields, amqp_params_network);
pp(amqp_msg, _) -> record_info(fields, amqp_msg);
pp('P_basic', _) -> record_info(fields, 'P_basic');
pp('exchange.declare', _) -> record_info(fields, 'exchange.declare');
pp('queue.declare', _) -> record_info(fields, 'queue.declare');
pp('queue.bind', _) -> record_info(fields, 'queue.bind');
pp('basic.consume', _) -> record_info(fields, 'basic.consume');
pp('basic.consume_ok', _) -> record_info(fields, 'basic.consume_ok');
pp('basic.publish', _) -> record_info(fields, 'basic.publish');
pp('basic.deliver', _) -> record_info(fields, 'basic.deliver');
pp(_, _) -> no.
