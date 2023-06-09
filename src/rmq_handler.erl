-module(rmq_handler).

%% API
-export([handle_msg/4]).

%% Behavior definition
-callback handle_msg(rmq:routing_key(),
                     Body :: binary(),
                     rmq:extra_handler_args()
) -> ack | nack | ok.

%%%
%%% API
%%%
-spec handle_msg(module(), rmq:routing_key(), Body :: binary(), rmq:extra_handler_args()) -> ack | nack | ok.
handle_msg(Handler, RK, Body, ExtraArgs) ->
    Handler:handle_msg(RK, Body, ExtraArgs).
