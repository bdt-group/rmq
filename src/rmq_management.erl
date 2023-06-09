-module(rmq_management).

-include_lib("kernel/include/logger.hrl").

%% API
-export([destroy_consumer/5]).
-export([destroy_producer/4]).
%% Types
-type vhost() :: rmq:vhost().
-type queue() :: rmq:queue().
-type exchange() :: exchange().
-type subject() :: queue | binding | exchange.
-type destroy_reason() :: {subject(), unexpected_bindings | {management_api, hapi:error_reason()}}.

-export_type([destroy_reason/0]).


%%%===================================================================
%%% API
%%%===================================================================

-spec destroy_consumer(uri_string:uri_map(), vhost(),
                       queue(), exchange(), pos_integer()) -> ok | {error, destroy_reason()}.
destroy_consumer(ManagementURI, VHost, Queue, BoundedExchange, Timeout) ->
    VHostStr = escape_vhost(binary_to_list(VHost)),
    QueueStr = binary_to_list(Queue),
    BoundedExchangeStr = binary_to_list(BoundedExchange),
    delete_bindings(ManagementURI, VHostStr, QueueStr, BoundedExchangeStr, Timeout).

-spec destroy_producer(uri_string:uri_map(), vhost(),
                       exchange(), pos_integer()) -> ok | {error, destroy_reason()}.
destroy_producer(ManagementURI, VHost, Exchange, Timeout) ->
    VHostStr = escape_vhost(binary_to_list(VHost)),
    ExchangeStr = binary_to_list(Exchange),
    delete_exchange(ManagementURI, VHostStr, ExchangeStr, Timeout).


%%%===================================================================
%%% Internal functions
%%%===================================================================

delete_bindings(ManagementURI, VHost, Queue, BoundedExchange, Timeout) ->
    case get_bindings_api(ManagementURI, VHost, Queue, BoundedExchange, Timeout) of
        {ok, []} ->
            ?LOG_WARNING("no bindings are found consumer ~p -> ~p on vhost ~p, ignore",
                         [BoundedExchange, Queue, VHost]),
            delete_queue(ManagementURI, VHost, Queue, Timeout);
        % explicitly check that there is only one binding on requested exchange/queue pair,
        % because rmq creates only a single one. Upon presense of other bindings - we stop, because
        % this bindings were created outside of rmq library and should be managed by hand
        {ok, [#{<<"properties_key">> := PropertyKey}]} ->
            PropertyKeyStr = binary_to_list(PropertyKey),
            case delete_binding_api(ManagementURI, VHost, Queue,
                                    BoundedExchange, PropertyKeyStr, Timeout) of
                ok ->
                    delete_queue(ManagementURI, VHost, Queue, Timeout);
                {error, Reason} ->
                    ?LOG_ERROR("unable to delete binding ~p -> ~p on vhost ~p (prop ~p) due to ~p",
                               [BoundedExchange, Reason, VHost, PropertyKey, Reason]),
                    {error, {bindings, {management_api, Reason}}}
            end;
        {ok, Bindings} ->
            ?LOG_ERROR("found unexpected bindings ~p for consumer ~p -> ~p on vhost ~p, stop",
                       [Bindings, BoundedExchange, Queue, VHost]),
            {error, {binding, unexpected_bindings}};
        {error, Reason} ->
            ?LOG_ERROR("unable to retrieve bindings for consumer ~p -> ~p on vhost ~p due to ~p",
                       [BoundedExchange, Queue, VHost, Reason]),
            {error, {binding, {management_api, Reason}}}
    end.

delete_queue(ManagementURI, VHost, Queue, Timeout) ->
    case delete_queue_api(ManagementURI, VHost, Queue, Timeout) of
        ok -> ok;
        {error, Reason} ->
            ?LOG_ERROR("unable to delete queue ~p on vhost ~p due to ~p, stop",
                       [Queue, VHost, Reason]),
            {error, {error, {management_api, Reason}}}
    end.

delete_exchange(ManagementURI, VHost, Exchange, Timeout) ->
    case delete_exchange_api(ManagementURI, VHost, Exchange, Timeout) of
        ok -> ok;
        {error, Reason} ->
            ?LOG_ERROR("unable to delete exchange ~p on vhost ~p due to ~p",
                       [Exchange, VHost]),
            {error, {exchange, {management_api, Reason}}}
    end.

get_bindings_api(ManagementURI, VHost, Queue, BoundedExchange, Timeout) ->
    URI = ManagementURI#{path := "/api/bindings/" ++ VHost ++ "/e/" ++
                             BoundedExchange ++ "/q/" ++ Queue},
    process_resp(hapi:get(URI, hapi_opts(URI, Timeout))).

delete_binding_api(ManagementURI, VHost, Queue, BoundedExchange, PropertyKey, Timeout) ->
    URI = ManagementURI#{path := "/api/bindings/" ++ VHost ++ "/e/" ++
                             BoundedExchange ++ "/q/" ++ Queue ++ "/" ++ PropertyKey},
    process_resp(hapi:delete(URI, hapi_opts(URI, Timeout))).

delete_exchange_api(ManagementURI, VHost, Exchange, Timeout) ->
    URI = ManagementURI#{path := "/api/exchanges/" ++ VHost ++ "/" ++ Exchange},
    process_resp(hapi:delete(URI, hapi_opts(URI, Timeout))).

delete_queue_api(ManagementURI, VHost, Queue, Timeout) ->
    URI = ManagementURI#{path := "/api/queues/" ++ VHost ++ "/" ++ Queue},
    process_resp(hapi:delete(URI, hapi_opts(URI, Timeout))).

% probably somewhere in libs there is more elegant solution for percent encoding,
% but I could not find one in stdlib, gun etc.
escape_vhost("/") -> "%2F";
escape_vhost(Other) -> Other.

hapi_opts(#{userinfo := UserInfo}, Timeout) ->
    [Username, Password] = string:split(UserInfo, ":"),
    #{timeout => Timeout,
      auth => #{type => basic, username => Username, password => Password}};

hapi_opts(_, Timeout) ->
    #{timeout => Timeout}.

process_resp(HapiRes) ->
    case HapiRes of
        {ok, {200, _, Data} = Resp} ->
            try jiffy:decode(Data, [return_maps]) of
                Decoded ->
                    {ok, Decoded}
            catch
                error:badarg ->
                    {error, {unexpected_response, Resp}}
            end;
        {ok, {204, _, _}} -> % no content returned for DELETE actions
            ok;
        {ok, {_OtherCode, _, _} = Resp} ->
            {error, {unexpected_response, Resp}};
        {error, _} = Error ->
            Error
    end.
