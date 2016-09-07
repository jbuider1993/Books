-module(intelDB_FileManager).

-behaviour(gen_server).

%% API
-compile(export_all).
%% gen_server callbacks
-export([
        init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3
    ]).

-record(state, {
        locked_set, % set of locked keys
        locks       % pid -> {monitor_ref, locked_keys} dict
    }).

-record(recLock, {
        lockres = 0 :: integer(), %% the resource Id that be locked, e
        pid :: pid(), %% which pid lock the lockres 
        rwLock = 0 :: integer(), %% the locked count
        lockpos = 0 :: integer(),  %% parameter is lock
        time       %% when be locked
    }).


-define(DEFAULT_MAX_SLEEP, 16).
-define(DEFAULT_TIMEOUT, 5000).
-define(LOCK_TIMEOUT, 1000).
-define(UNLOCK_TIMEOUT, 10*?LOCK_TIMEOUT).

%%%===================================================================
%%% API
%%%===================================================================

start() ->
	Name = "",
    start_link(Name, []).

start_link(Name, Options) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Name, Options], []).

readLock(ResId, Pos) ->
    readLockLoop(ResId, Pos, ?DEFAULT_TIMEOUT, ?DEFAULT_MAX_SLEEP).

readLock(ResId) ->
    readLockLoop(ResId, 0, ?DEFAULT_TIMEOUT, ?DEFAULT_MAX_SLEEP).

readUnLock(ResId, Pos) ->
   read_UnLock(ResId, Pos).

readUnLock(ResId) ->
   read_UnLock(ResId, 0).

writeLock(ResId) ->
    writeLockLoop(ResId, 0, ?DEFAULT_TIMEOUT, ?DEFAULT_MAX_SLEEP).

writeLock(ResId, Pos) ->
    writeLockLoop(ResId, Pos, ?DEFAULT_TIMEOUT, ?DEFAULT_MAX_SLEEP).

writeUnLock(ResId, Pos) ->
   write_UnLock(ResId, Pos).

writeUnLock(ResId) ->
   write_UnLock(ResId, 0).

   
   
%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Name, Options]) ->
    {ok, #state{locked_set = sets:new(), locks = dict:new()}}.

handle_call({rlock, Res_id, Lockpos}, {Pid, _Tag}, #state{ locked_set = LockedSet, locks = LocksDict } = State) ->
    {Reslt, NewState} =
     case dict:find(Res_id, LocksDict) of
       {ok, #recLock{rwLock = RW_Lock, lockpos = Lockpos1}} ->
            if (RW_Lock >=0) ->
               NewRecord = #recLock{ pid = Pid, rwLock = RW_Lock + 1, time = now(), lockpos = Lockpos1},            
               {ok, #state{locked_set = LockedSet, locks = dict:store(Res_id, NewRecord, LocksDict)}};
            true -> 
               {failed, State}
            end;              
       error -> 
               NewRecord = #recLock{ pid = Pid, rwLock = 1,time = now(), lockpos = Lockpos},            
               {ok, #state{locked_set = LockedSet, locks = dict:store(Res_id, NewRecord, LocksDict)}}            
    end,
    {reply, Reslt, NewState};
   
handle_call({rUnlock, Res_id, Lockpos}, {Pid, _Tag}, #state{ locked_set = LockedSet, locks = LocksDict } = State) ->
    {Reslt, NewState} =
     case dict:find(Res_id, LocksDict) of
       {ok, #recLock{rwLock = RW_Lock, lockpos = Lockpos1}} ->
            if ((RW_Lock -1) =< 0) ->               
               {ok, #state{locked_set = LockedSet, locks = dict:erase(Res_id, LocksDict)}};
            true -> 
               NewRecord = #recLock{ pid = Pid, rwLock = RW_Lock -1, time = now(), lockpos = Lockpos1},            
               {ok, #state{locked_set = LockedSet, locks = dict:store(Res_id, NewRecord, LocksDict)}}
            end;              
       error -> 
               {ok, State}            
    end,
    {reply, Reslt, NewState};


handle_call({wlock, Res_id, Lockpos}, {Pid, _Tag}, #state{ locked_set = LockedSet, locks = LocksDict } = State) ->
    {Reslt, NewState} =
     case dict:find(Res_id, LocksDict) of
       {ok, #recLock{rwLock = RW_Lock, lockpos = Lockpos1} = Value} ->
            if (RW_Lock == 0) ->
               NewRecord = #recLock{rwLock = -1, time = now(), lockpos = Lockpos1},            
               {ok, #state{locked_set = LockedSet, locks = dict:store(Res_id, NewRecord, LocksDict)}};
            true -> 
               {failed, State}
            end;              
        error -> 
               NewRecord = #recLock{ rwLock = -1, time = now(), lockpos = Lockpos},            
               {ok, #state{locked_set = LockedSet, locks = dict:store(Res_id, NewRecord, LocksDict)}}            
    end,
    {reply, Reslt, NewState};


handle_call({wUnlock, Res_id, Lockpos}, {Pid, _Tag}, #state{ locked_set = LockedSet, locks = LocksDict } = State) ->
    {Reslt, NewState} =
     case dict:find(Res_id, LocksDict) of
       {ok, Value} ->
               {ok, #state{locked_set = LockedSet, locks = dict:erase(Res_id, LocksDict)}};
       error -> 
               {ok, State}            
    end,
    {reply, Reslt, NewState};


handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _Monitor, process, Pid, _Reason}, #state{ locked_set = LockedSet, locks = LocksDict } = State) ->
    {ok, {_, PidKeySet}} = dict:find(Pid, LocksDict),
    NewLockedSet = sets:subtract(LockedSet, PidKeySet),
    NewLocksDict = dict:erase(Pid, LocksDict),
    {noreply, State#state{ locked_set = NewLockedSet, locks = NewLocksDict }};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

read_Lock(ResId, Pos) ->
	gen_server:call(?MODULE, {rlock, ResId, Pos},  infinity).

read_UnLock(ResId, Pos) ->
	gen_server:call(?MODULE, {rUnlock, ResId, Pos},  infinity).

write_Lock(ResId, Pos) ->
	gen_server:call(?MODULE, {wlock, ResId, Pos},  infinity).

write_UnLock(ResId, Pos) ->
	gen_server:call(?MODULE, {wUnlock, ResId, Pos},  infinity).


readLockLoop(ResId, Pos, Timeout, MaxSleep) when Timeout < 0 ->
    false;

readLockLoop(ResId, Pos, Timeout, MaxSleep) ->
   case read_Lock(ResId, Pos) of
      failed -> 
            Sleep = random:uniform(MaxSleep),
            NewTimeout = Timeout - Sleep,
            if 
              NewTimeout >= 0 -> 
                  timer:sleep(Sleep); 
            true -> 
                  ok 
            end,
            readLockLoop(ResId, Pos, NewTimeout, MaxSleep);
       ok ->
            true
       end.

writeLockLoop(ResId, Pos, Timeout, MaxSleep) when Timeout < 0 ->
    false;

writeLockLoop(ResId, Pos, Timeout, MaxSleep) ->
    case write_Lock(ResId, Pos) of
      failed -> 
            Sleep = random:uniform(MaxSleep),
            NewTimeout = Timeout - Sleep,
            if 
              NewTimeout >= 0 -> 
                  timer:sleep(Sleep);
            true -> 
                  ok 
            end,
            writeLockLoop(ResId, Pos, NewTimeout, MaxSleep);
       ok ->
            true
       end.


%%==================================================================================================
%%==================================================================================================
	   
	   
test() ->
   start(),
   ResId = "100",
   writeLock(ResId),
   Open = 1200 + 23,
   writeUnLock(ResId),
   OpenOP = 1200 + 232.

test1() ->
   %%start(),
   ResId = "100",
   writeLock(ResId),
   Open = 1200 + 23,
   writeUnLock(ResId),
   OpenOP = 1200 + 232.



   
   

   
	   