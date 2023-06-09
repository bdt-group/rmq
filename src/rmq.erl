-module(rmq).

-behaviour(application).

%% API
-export([start/0, stop/0]).
-export([child_spec/3, child_spec/4]).
-export([send/5, send/6]).
-export([default_config/0]).
-export([default_create_config/1]).
-export([default_create_config/2]).
-export([set_amqp_client_timeout/1]).
%% Application callbacks
-export([start/2, stop/1, config_change/3]).
%% Types
-export_type([config/0, consumer_create_config/0, producer_create_config/0]).
-export_type([send_opts/0]).
-export_type([exchange/0, exchange_type/0]).
-export_type([routing_key/0, queue/0]).
-export_type([vhost/0]).
-export_type([connection_name/0]).
-export_type([worker_type/0]).
-export_type([extra_handler_args/0]).

-type send_opts() :: #{content_type => binary()}.
-type config() :: consumer_config() | producer_config().
-type consumer_config() :: #{type := consumer,
                             handler := module(),
                             queue := queue(),
                             ack := boolean(),
                             create => consumer_create_config(),
                             prefetch_count => non_neg_integer()}.
-type producer_config() :: #{type := producer,
                             exchange := exchange(),
                             create => producer_create_config()}.
-type worker_type() :: producer | consumer.
-type exchange() :: binary().

%% should we add here 'match' and 'headers':
%% https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchanges
-type exchange_type() :: direct | topic | fanout | {custom, atom()}.
-type x_queue_type() :: classic | quorum.

-type routing_key() :: binary().
-type queue() :: binary().
-type vhost() :: binary().
-type connection_name() :: atom() | {via, atom(), term()}.
-type consumer_create_config() :: #{exchange => exchange(),
                                    queue_durable => boolean(),
                                    queue_exclusive => boolean(),
                                    queue_auto_delete => boolean(),
                                    queue_nowait => boolean(),
                                    queue_binding_key => binary(),
                                    queue_x_expires => pos_integer(),
                                    queue_x_message_ttl => pos_integer(),
                                    queue_x_max_length => pos_integer(),
                                    queue_x_max_length_bytes => pos_integer(),
                                    queue_x_queue_type => x_queue_type(),
                                    queue_x_quorum_initial_group_size => pos_integer()}.

-type producer_create_config() :: #{exchange_type => exchange_type(),
                                    exchange_durable => boolean(),
                                    exchange_auto_delete => boolean(),
                                    exchange_nowait => boolean(),
                                    exchange_arguments => list()}.
-type extra_handler_args()     :: term().

%%%===================================================================
%%% API
%%%===================================================================
-spec start() -> ok | {error, term()}.
start() ->
    case application:ensure_all_started(?MODULE) of
        {ok, _} -> ok;
        {error, _} = Err -> Err
    end.

-spec stop() -> ok | {error, term()}.
stop() ->
    case application:stop(?MODULE) of
        ok -> ok;
        {error, {not_started, _}} -> ok;
        {error, _} = Err -> Err
    end.

-spec send(connection_name(), exchange(), routing_key(),
           binary(), send_opts()) -> ok | {error, noproc}.
send(ConnName, Exchange, RoutingKey, Data, Opts) ->
    send(ConnName, Exchange, RoutingKey, Data, Opts, async).

-spec send(connection_name(), exchange(), routing_key(),
           binary(), send_opts(), sync | async) -> ok | {error, noproc}.
send(ConnName, Exchange, RoutingKey, Data, Opts, Sync) ->
    rmq_srv:send(ConnName, Exchange, RoutingKey, Data, Opts, Sync).

-spec child_spec(uri_string:uri_map(), connection_name(), config()
) -> supervisor:child_spec().
child_spec(URI, Name, Config) ->
    child_spec(URI, Name, Config, []).

-spec child_spec(uri_string:uri_map(), connection_name(), config(), extra_handler_args()
) -> supervisor:child_spec().
child_spec(URI, Name, Config, ExtraArgs) ->
    rmq_sup:child_spec(URI, Name, Config, ExtraArgs).

-spec default_config() -> map().
default_config() ->
    #{ack => false, handler => undefined}.

-spec default_create_config(producer) -> producer_create_config().
default_create_config(producer) ->
    #{exchange_type => topic,
      exchange_durable => true,
      exchange_auto_delete => false,
      exchange_nowait => false,
      exchange_arguments => []}.

-spec default_create_config(consumer, queue()) -> consumer_create_config().
default_create_config(consumer, Queue) ->
    #{queue_durable => false,
      queue_exclusive => false,
      queue_auto_delete => false,
      queue_nowait => false,
      queue_binding_key => Queue}.

-spec set_amqp_client_timeout(timeout()) -> ok.
set_amqp_client_timeout(Timeout) ->
    application:set_env(amqp_client, gen_server_call_timeout, Timeout).

%%%===================================================================
%%% Application callbacks
%%%===================================================================
-spec start(normal | {takeover, node()} | {failover, node()}, term()) ->
                   {ok, pid()} | {error, term()}.
start(_StartType, _StartArgs) ->
    rmq_sup:start_link().

-spec stop(term()) -> any().
stop(_State) ->
    ok.

-spec config_change(Changed :: [{atom(), term()}],
                    New :: [{atom(), term()}],
                    Removed :: [atom()]) -> ok.
config_change(_Changed, _New, _Removed) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================
