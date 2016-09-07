-module(intelDB_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-spec start_link() -> {ok, pid()}.
start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    application:start(ranch),
    application:start(tcp_echo),	
	Procs = [
		{intelDB_TagBase, {intelDB_TagBase, start_link, ['aaaa', [ddd,www]]}, permanent, 5000, worker, [intelDB_TagBase]},
		{intelDB_ArchManager, {intelDB_ArchManager, start_link, ['aaaa', [ddd,www]]}, permanent, 5000, worker, [intelDB_ArchManager]},
		{intelDB_BuffManager, {intelDB_BuffManager, start_link, ['aaaa', [ddd,www]]}, permanent, 5000, worker, [intelDB_BuffManager]},
		{intelDB_TempManager, {intelDB_TempManager, start_link, ['aaaa', [ddd,www]]}, permanent, 5000, worker, [intelDB_TempManager]},
		{intelDB_Index, {intelDB_Index, start_link, ['aaaa', [ddd,www]]}, permanent, 5000, worker, [intelDB_Index]},
		{intelDB_BuffTrigger, {intelDB_BuffTrigger, start_link, ['aaaa', [ddd,www]]}, permanent, 5000, worker, [intelDB_BuffTrigger]}
	],
	{ok, {{one_for_one, 10, 10}, Procs}}.