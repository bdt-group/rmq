-module(rmq_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).
-export([child_spec/4]).
%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================
-spec start_link() -> {ok, pid()} | {error, term()} | ignore.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================
-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    SupFlags = #{strategy => one_for_one,
                 intensity => 10,
                 period => 1},
    {ok, {SupFlags, []}}.

-spec child_spec(uri_string:uri_map(), rmq:connection_name(),
                 rmq:config(), rmq:extra_handler_args()
) -> supervisor:child_spec().
child_spec(URI, ConnName, Config, ExtraArgs) ->
    #{id => {rmq_srv, ConnName},
      start => {rmq_srv, start_link, [URI, ConnName, Config, ExtraArgs]},
      restart => permanent,
      shutdown => 5000,
      type => worker,
      modules => [rmq_srv]}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
