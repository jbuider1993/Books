-module(intelDB_RTBase).
-author('Jie-Liu').

-behaviour(gen_server).
-compile(export_all).

-define(DEF_TIMEOUT, 30000).
-define(DEF_TAGCOUNT, 1000000).
-define(DEF_BUFFERSIZE,40960).
-define(DEF_DELETETIME, 2000).

-define(DEF_OUTFILE, "C:/1/data/base.dat").
 
-record(idx_value, {index::integer(), time::integer(), value::float(), status::integer()}).
-record(tag_value, {time::integer(), value::float(), status::integer()}).
-record(state, {values                ,    %% real time values of tags
                update_time  = 0      :: integer(),  %% last time the values is updated 
				update_index = 0      :: integer(),  %% which is updated                   				
                tag_count    = 0      :: integer(),  %% the count of tag = max of index + 1
				time_zone    = 0      :: integer(),
				fd                    , %% for exter file
                errors = []           :: list()               
               }). 
 
 
 %%%PUBLIC API
 
 putRecord(Idx_value) ->
     gen_server:call(?MODULE, {putRecord, Idx_value}, ?DEF_TIMEOUT). 

 getRecord(Index) -> 
     case gen_server:call(?MODULE, {getRecord, Index}, ?DEF_TIMEOUT) of 
	   {ok, Tag_value} -> Tag_value;
	   error -> error
	 end.
 
updateTime(Idx, Time) ->
	gen_server:call(?MODULE, {updateTime,[Idx, Time]}, ?DEF_TIMEOUT).
	
updateTimes(Idxs, Time) -> 	
	gen_server:call(?MODULE, {updateTimes,[Idxs, Time]}, ?DEF_TIMEOUT).


 getNodeStatue() -> 
     {ok, Statue}  = gen_server:call(?MODULE, getNodeStatue, ?DEF_TIMEOUT).
	 

%%=======================================================================
%% get from extern file
%%======================================================================= 	 
 getFromFile() ->
     gen_server:call(?MODULE, getFromFile, infinity).

%%=======================================================================
%% save to extern file
%%======================================================================= 	 
 putToFile() ->
     gen_server:call(?MODULE, putToFile, infinity).
	 
	 
	 
 start_link(Name, Options) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Name, Options], []).

 
 start() ->
    Name = ?DEF_OUTFILE,
    start_link(Name, []).

 stop() ->   	
   gen_server:cast(?MODULE, stop).

	
 init([Name, Options]) ->
            Tag_count = ?DEF_TAGCOUNT,
			case file:open(Name, [read, write, raw, binary, {delayed_write, ?DEF_BUFFERSIZE, ?DEF_DELETETIME}]) of
			  {ok, Fd} -> 
                 {ok, #state{ values = array:new(Tag_count), fd = Fd, tag_count = Tag_count
                       }};
              {error, Reason} -> 
                 {ok, #state{ values = array:new(Tag_count),
			             tag_count = Tag_count
                       }}
			end.		   

 getTagarray(Fd, Tcount, Tcount, Tagarray) ->
	Tagarray;
	
 getTagarray(Fd, N, Tcount, Tagarray) ->
      {ok, <<Time:64/integer, Value:64/float, Status:8/integer>>} = file:read(Fd, 8 + 8 + 1),
      Tagvalue = #tag_value{time = Time, value = Value, status = Status},	 
	  Tagarray1 = array:set(N, Tagvalue, Tagarray),
	  getTagarray(Fd, N + 1, Tcount, Tagarray1).
 
 putValueToFile(Fd, Tagcount, Tagcount, Tagvalues) ->
     ok;
 putValueToFile(Fd, N, Tagcount, Tagvalues) ->
	  case array:get(N, Tagvalues) of
	     undefined -> putValueToFile(Fd, N, Tagcount, Tagvalues);
		 Tagvalue  ->
                     Time = Tagvalue#tag_value.time,
					 Value = Tagvalue#tag_value.value,
                     Status = Tagvalue#tag_value.status,		 
		             case file:write(Fd, <<Time:64/integer, Value:64/float, Status:8/integer>>) of
                           ok -> putValueToFile(Fd, N+1, Tagcount, Tagvalues);	  
                           {error, Reason} -> {error, Reason}
                      end
      end.					  

updateIndexTime([], Time, State) ->
     State;  
updateIndexTime([Idx | Rest], Time, #state{values = Tagvalues, tag_count = Tagcount} = State) ->
    if Idx >=0, Idx < Tagcount, Time >0  ->
       Tag_value = array:get(Idx, Tagvalues),
	   if Time > Tag_value#tag_value.time ->
		  Value  = Tag_value#tag_value.value,
		  Statue = Tag_value#tag_value.status,
		  Tag_value1 = #tag_value{time = Time, value = Value, status = Statue},
		  Values1 = array:set(Idx, Tag_value1, Tagvalues),
		  State1 = #state{values = Values1, update_time = State#state.update_time, update_index = State#state.update_index, fd = State#state.fd, tag_count = State#state.tag_count, time_zone = State#state.time_zone},
		  updateIndexTime(Rest, Time, State1);
       true ->
	      updateIndexTime(Rest, Time,State)
	   end;
    true ->	   
	      updateIndexTime(Rest, Time,State)
    end.
    	  
	  
handle_call({updateTimes,[Idxs, Time]}, _From, State) ->
    State1 = updateIndexTime(Idxs, Time, State),
    {reply, {ok, Time}, State1};
	  

handle_call({updateTime,[Idx, Time]}, _From, #state{values = Tagvalues, tag_count=Tagcount} = State) ->
    if Idx >=0, Idx < Tagcount, Time >0  ->
       Tag_value = array:get(Idx, Tagvalues),
	   if Time > Tag_value#tag_value.time ->
		  Value  = Tag_value#tag_value.value,
		  Statue = Tag_value#tag_value.status,
		  Tag_value1 = #tag_value{time = Time, value = Value, status = Statue},
		  Values1 = array:set(Idx, Tag_value1, Tagvalues),
		  State1 = #state{values = Values1, update_time = State#state.update_time, update_index = State#state.update_index, fd = State#state.fd, tag_count = State#state.tag_count, time_zone = State#state.time_zone},
          {reply, {ok, Time}, State1};
	  true ->
	      {reply, errorTime, State}
	  end;
     true ->
	   {reply, errorIndex, State}
    end;


handle_call(putToFile, _From, #state{values = Tagvalues, fd = Fd, tag_count=Tagcount} = State) ->
	file:position(Fd, {bof, 0}),
	case file:write(Fd, <<Tagcount:64/integer>>) of
	     ok -> case putValueToFile(Fd, 0, Tagcount, Tagvalues) of
		          ok -> {reply, ok, State};
				  {error, Reason} -> {reply, {error, Reason}, State}
				end;  
		{error, Reason} -> 
		       {reply, {error, Reason}, State}
    end;			   
			
 handle_call(getFromFile, _From, State) ->
    Fd = State#state.fd,
	if Fd /= undefined ->
	   file:position(Fd,{bof,0}),
	   case file:read(Fd, 8) of
	     {ok, Data} ->
		      if is_binary(Data) ->
			     <<Tcount:64/integer>> = Data,
				 Tagarray = array:new(Tcount),
				 Tagarray1 = getTagarray(Fd, 0, Tcount, Tagarray),
				 State1 = State#state{values = Tagarray1, fd = Fd},
	             {reply, {ok, Tcount}, State1};
              true ->				 
	             {reply, {ok, error}, State}
			  end;
          {error, Reason} ->
	             {reply, {error, Reason}, State}
		end;
		true ->
	        {reply, {ok, error}, State}
    end;
					   
 handle_call({putRecord, Idx_value}, _From, State) ->
    #idx_value{index = Index, time = Time, value = Value, status = Status} = Idx_value,
	#state{values = Values, tag_count = Tag_count, fd = Fd} = State,
	if  Index >= 0, Index < Tag_count ->
       Tag_value = #tag_value{time = Time, value = Value, status = Status},
	   Values1 = array:set(Index, Tag_value, Values),
	   State1 = #state{values = Values1, update_time = erlang:now(), update_index = Index, tag_count = Tag_count, fd = Fd},
	   {reply, ok, State1};
	   true -> 
	   {reply, {ok, error}, State}
	end;

 handle_call({getRecord, Index}, _From, #state{values = Values, tag_count = Tag_count} = State) ->
    if Index >=0, Index < Tag_count ->
       Tag_value = array:get(Index, Values),
       {reply, {ok, Tag_value}, State};
     true ->
	   {reply, error, State}
    end;
 	
handle_call(getNodeStatue, _From, State) ->
	{reply, ok, State}.


terminate(_Reason, _State) ->
    ok.
    
handle_info(_Info, State) ->
    {noreply, State}.	
 	
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.	

handle_cast(stop, State) ->
   Fd = State#state.fd,
   file:close(Fd),
   {stop, normal, State}.
	
	
stamptomsecs({MegaSecs,Secs,MicroSecs} = Timestamp) ->
  (MegaSecs*1000000 + Secs)*1000000 + MicroSecs.	
	
	
	
	
	
	
%%======================================================================
%% === test the function ===============================================
%%======================================================================
	
	
loadValue(0) ->
   ok; 
loadValue(N) ->
  Idxvalue = #idx_value{index = N-1, time = stamptomsecs(now()), value = N + 234.23, status =1},
  putRecord(Idxvalue),
  loadValue(N-1).
  
getvalue(Index) ->
  getRecord(Index).
	
loadValues() ->
  N = ?DEF_TAGCOUNT,
  loadValue(N).	
 
testput() ->
  Idxvalue = #idx_value{index = 1000, time = stamptomsecs(now()), value = 234.23, status =1},
  putRecord(Idxvalue).

testget(Index) ->
  getRecord(Index).
