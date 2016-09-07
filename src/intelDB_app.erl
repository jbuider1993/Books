-module(intelDB_app).
-behaviour(application).

-export([start/2]).
-export([stop/0]).

start(_, _) ->
	{ok, NodeName} = application:get_env(intelDB, nodename),
	net_kernel:start([NodeName, longnames]),
	intelDB_sup:start_link().

stop() ->
	ok.