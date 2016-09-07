%% @author jliu4
%% @doc @todo Add description to intelDB_Server.


-module(intelDB_Server).

%% ====================================================================
%% API functions
%% ====================================================================
-behaviour(gen_server).
-compile(export_all).

-define(MAXBUFFREC, 200000).

-record(state, {
                pid,
				buffQueue = {} :: queue() | undefined, %% the buffer queue for income data
                count = 0 :: integer,    %% the count of element of buffer queue
                maxRecord :: integer,    %% the max number element of the queue.
                errors = []   :: list               
               }).

addDataRec(TagIndex, Time, Value, State) ->
	%%ok = gen_server:cast(?MODULE, {addDataRec, TagIndex, Time, Value, State}),	
	ok = gen_server:call(?MODULE, {addDataRec, TagIndex, Time, Value, State}, infinity),	
	ok.
	
tranDataRec() ->
	gen_server:call(?MODULE, {tranDataRec}, infinity).
	%%gen_server:cast(?MODULE, {tranDataRec}).


	 	
%% ====================================================================
%% Internal functions
%% ====================================================================

init([Name, Options]) ->
	Queue = queue:new(),			
	Pid = spawn_link(fun() -> tranDataToTemp() end),		
	State = #state{buffQueue = Queue, maxRecord = ?MAXBUFFREC, pid = Pid},
	%%Pid ! { self(), tranData},
	%%State = #state{buffQueue = Queue, maxRecord = ?MAXBUFFREC},
	erlang:send_after(60 * 1000, self(), {gc}),
    {ok, State}.

%%  NQueue = queue:in(Data, Queue). in the element in the rear
%%  {{value, Item}, NQueue} = queue:out(Queue). return front element 
%%  NQueue = queue:drop(Queue). remove front element
%%

tranDataToTemp() ->
    tranDataRec(),
    timer:sleep(1),
	tranDataToTemp().
	

start_link(Name, Options) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Name, Options], []).

start() ->
	Name = "",
    start_link(Name, []).

stop() ->   	
   gen_server:cast(?MODULE, stop).


terminate(_Reason, _State) ->
    ok.
    
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_cast({addDataRec, TagIndex, Time, Value, State}, _State) ->
   #state{count= Count, maxRecord = MaxRecord} = _State,          
   Data = <<TagIndex:64/integer, Time:64/integer, Value:64/integer, State:8/integer>>,
   if Count < MaxRecord ->
       Queue = _State#state.buffQueue,       
       NQueue = queue:in(Data, Queue),   
       {noreply, _State#state{buffQueue = NQueue, count = Count + 1}};
    true ->
	   intelDB_TempManager:addRecordToTemp(TagIndex, Time, Value, State, Data),	
       {noreply, _State}
	end;   	
    
	
handle_cast({tranDataRec}, _State) ->
   Count = _State#state.count,
   if Count == 0 ->
      {reply, {isEmpty}, _State};
   true ->
       Queue = _State#state.buffQueue,
      {{value, Data}, NQueue} = queue:out(Queue),

	  %%================================== only for test===================================== 
	   <<TagIndex:64/integer, Time:64/integer, Value:64/integer, State:8/integer>> = Data,     
	   io:format("~w============================================~w~n", [TagIndex, Count]),	   
	   intelDB_TempManager:addRecordToTemp(TagIndex, Time, Value, State, Data),
	  %%===================================================================================== 
	  
      _NState = _State#state{buffQueue = NQueue, count = Count -1},	  
      {reply, {ok}, _NState}
   end;  	

handle_cast(stop, State) ->
   #state{buffQueue = Queue, pid = Pid} = State,   
   exit(Pid, kill),
   erlang:garbage_collect(),
   {stop, normal, State}.

   
handle_call({tranDataRec}, _From, _State) ->
   Count = _State#state.count,
   if Count == 0 ->
      {reply, {isEmpty}, _State};
   true ->
       Queue = _State#state.buffQueue,
      {{value, Data}, NQueue} = queue:out(Queue),

	  %%================================== only for test===================================== 
	   <<TagIndex:64/integer, Time:64/integer, Value:64/integer, State:8/integer>> = Data,     
	   io:format("~w============================================~w~n", [TagIndex, Count]),	   
	   intelDB_TempManager:addRecordToTemp(TagIndex, Time, Value, State, Data),
	  %%===================================================================================== 
	  
      _NState = _State#state{buffQueue = NQueue, count = Count -1},	  
      {reply, {ok}, _NState}
   end;  
   
handle_call({addDataRec, TagIndex, Time, Value, State}, _From, _State) ->
   #state{count= Count, maxRecord = MaxRecord} = _State,          
   if Count < MaxRecord ->
       Queue = _State#state.buffQueue,
       Data = <<TagIndex:64/integer, Time:64/integer, Value:64/integer, State:8/integer>>,	   	   
	   %%intelDB_TempManager:addRecordToTemp(TagIndex, Time, Value, State, Data),
       NQueue = queue:in(Data, Queue),   
       {reply, ok, _State#state{buffQueue = NQueue, count = Count + 1}};
    true ->
	   Queue = _State#state.buffQueue,
	   {{value, Data}, NQueue} = queue:out(Queue),
	   intelDB_TempManager:addRecordToTemp(TagIndex, Time, Value, State, Data),
       NData = <<TagIndex:64/integer, Time:64/integer, Value:64/integer, State:8/integer>>,
       {reply, ok, _State#state{buffQueue = queue:in(NData, NQueue)}}  
    end.

handle_info({gc}, State) ->
	Pid = State#state.pid,
    erlang:garbage_collect(Pid),
    erlang:garbage_collect(self()),
    erlang:send_after(20 * 1000, self(), {gc}),
    {noreply, State};	
	
handle_info({gc1}, State) ->
   case erlang:memory(binary) of
       % We use more than 100 MB of binary space
       Binary when Binary > 100000000 ->
	       Pid = State#state.pid,
           erlang:garbage_collect(Pid);
       _ ->
           ok
   end,
   erlang:send_after(10 * 1000, self(), {gc}),
   {noreply, State}.
   

%%===========================================================================
%% for test functions
%%===========================================================================

add() ->
	start(),
	Name = "temp",
	%%intelDB_IndexManager:start_link(Name, []),
	intelDB_ArchManager:start_link(Name, []),
	intelDB_TempManager:start_link(Name, []),	
	addRecord(0).

addRecord(N) ->
  Index = 102, %%random:uniform(500),
  Time  =  intelDB_RTBase:stamptomsecs(now()),
  Value = random:uniform(500),
  State = 1,
  addDataRec(Index, Time, Value, State), 
  
  if N >= 100000 ->
	 timer:sleep(10),
	 addRecord(0);
  true -> 	 
	 timer:sleep(1),
     addRecord(N + 1)
  end.