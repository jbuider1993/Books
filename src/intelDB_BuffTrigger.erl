%% @author Administrator
%% @doc @todo Add description to intelDB_BuffTrigger.


-module(intelDB_BuffTrigger).


-behaviour(gen_server).
-compile(export_all).

-include_lib("stdlib/include/qlc.hrl").

-define(MEMTAGTIME, memIndexTime).
-define(MAXBUFFTIME, 10*60*1000000).
-define(MAXINTEGER, 16#FFFFFFFFFFFFFFFF).


%%=============================================================
%%  PUBLIC API
%%=============================================================

start_link(Name, Options) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Name, Options], []).

start() ->
    start_link("", []).

stop() ->   	
   gen_server:cast(?MODULE, stop).

addTagTime(Index) ->
   gen_server:cast(?MODULE, {addTagTime, Index}).

init() ->
    TagCount = intelDB_TagBase:getTagCount(),
    ets:new(?MEMTAGTIME,  [ordered_set, named_table, {keypos, 1}]), 	
    [ets:insert(?MEMTAGTIME, [{N, ?MAXINTEGER}]) || N <- lists:seq(0, TagCount)],
	erlang:send_after(60 * 1000, self(), {trigMoveBuff}),
    {ok, []}.
		
init([Name, Options]) ->
    TagCount = intelDB_TagBase:getTagCount(),
    ets:new(?MEMTAGTIME,  [ordered_set, named_table, {keypos, 1}]), 	
    [ets:insert(?MEMTAGTIME, [{N, ?MAXINTEGER}]) || N <- lists:seq(0, TagCount)],
	erlang:send_after(60 * 1000, self(), {trigMoveBuff}),
    {ok, []}.

%%=============================================================
%% Handle case  
%%=============================================================

terminate(_Reason, _State) ->
    ok.
    
handle_info({trigMoveBuff}, State) ->
   moveBuffToFile(intelDB_util:stamptomsecs(now()) - ?MAXBUFFTIME),
   erlang:send_after(60 * 1000, self(), {trigMoveBuff}),
   {noreply, State};
   
handle_info(_Info, State) ->
    {noreply, State}.	
 	
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_cast({addTagTime, Index}, State) ->
   Time = intelDB_util:stamptomsecs(now()),
   ets:update_element(?MEMTAGTIME, Index, {2, Time}),
   {noreply, State};   

handle_cast(stop, State) ->
   {stop, normal, State}.

moveBuffToFile(StartMoveTime) ->
    MoveList = ets:foldl(fun({Index, LastTime}, MList) ->			
							if StartMoveTime > LastTime ->
							   ets:update_element(?MEMTAGTIME, Index, {2, ?MAXINTEGER}),   
							   [Index | MList];
							true ->
							   MList			
							end
			             end, [], ?MEMTAGTIME),
	if length(MoveList) > 0 ->  
	   intelDB_BuffManager:moveToTemp(MoveList);
    true ->
       ok
    end.
	
						 