%% @author jliu4
%% @doc @todo Add description to intelDB_TempManager.


-module(intelDB_TempManager).

-author('Jie-Liu').



%% ===========================================================================================================
%% Temp data File
%% Header :
%% <<?VERSION:32/integer, ?ARCHIVE_FILE:8/integer(FILE TYPE), FIdx:32/integer, Unused:64/integer>>
%% TEMP_HEADER = (32 + 8 + 32 + 64)
%% <<IndexPos:64/integer, BlockPos:64/integer>>
%% Index area : from top to end
%% INDEX_SIZE = (64 + 64) div 8
%% <<TagIndex:64/integer, Pos:64/integer, extern:64/integer>> 64 + 64 
%% Block area : from end to top
%% Head
%% HEADER_SIZE = (32 + (64 + 64 + 8 + 64) + (64 + 64 + 8 + 64) + 64) div 8
%% FIRREC_SIZE = (32 + (64 + 64 + 8 + 64)) div 8    
%% <<CRC32:32/integer>> 
%% <<FTime:64/integer, FValue:64/integer, FState:8/integer, extern:64/integer>>
%% SECOND_SIZE = ((64 + 64 + 8 + 64) + 64) div 8
%% <<LTime:64/integer, LValue:64/integer, LState:8/integer, extern:64/integer>>
%% <<Length:64/integer>>
%% body
%% <<Data/binary>>
%% CRC(Index)
%% block size is : 32(CRC) + (64 + 8 + 64) + 2*(64 + 64 + 8 + 64) + 64 + Length(Data)
%% ============================================================================================================


-behaviour(gen_server).
-compile(export_all).

-include_lib("stdlib/include/qlc.hrl").

-define(VERSION, 1000000).
-define(TEMP_FILE, 200).
-define(MAXSECLEN, 24*1024).
-define(DEF_BUFFERSIZE,10240).
-define(DEF_DELETETIME, 2000).
-define(MAXHANDLE, 20).
-define(TEMP_HEADER, (32 + 8 + 32 + 64) div 8).
-define(INDEX_SIZE, (64 + 64 + 64) div 8).
-define(HEADER_SIZE, (32 + (64 + 64 + 8 + 64) + (64 + 64 + 8 + 64) + 64) div 8).
-define(FIRREC_SIZE, (32 + (64 + 64 + 8 + 64)) div 8).
-define(TEMP_SECT, (32 + (64 + 8 + 64) + 2*(64 + 64 + 8 + 64) + 64) div 8).
-define(TEMP_POSSEC, (64 + 64) div 8).
-define(MAX_TEMPSIZE, 1024*1024*1024).

-define(DEF_TEMPDIR, "c:/IntelDB/data/tempBase/").
-define(TEMP_PREX, "Temp").
-define(FREE_PREX, "TempSpace").
-define(TAGS_PREX, "TempTag").
-define(TEMP_FILES, "TempFiles").


-define(OPEND_FILE, tempOpenfiles).
-define(MEMTEMPFILES, memTempFiles).
-define(TEMPTAGS, tempTags).
-define(MINBLOCK_SIZE, ?INDEX_SIZE + ?HEADER_SIZE).
-define(MAXINTEGER, 16#FFFFFFFFFFFFFFFF).

-record(pos_point, {file_id :: integer,
					pos :: integer
						}).

%=========================================================
% time_sec size = 64 + 64
%=========================================================
-record(time_sec, { start_time :: integer,
					end_time   :: integer
					}).

%=========================================================
% node_point size = 32 + 64 + 64 + 64
%=========================================================

-record(node_point, {
					 time :: #time_sec{}, pos :: #pos_point{}
					 }).

-record(tempFiles, {               
                  file_idx = 0 :: integer, %% the file_idx cannot be changed in life time.
                  file_id  = 0 :: integer,  %% the file_id can be changed.
                  path :: list
				 }).

-record(sequence, {
				 id = 0 :: integer,   %% the Id of squence 0 : Index File;
                 value = 0 :: integer %% the squence value
				 } ).

-record(freespace, {file_id   :: integer,   %% the file_id can be changed
					free_size :: integer,
					index_pos :: pos_integer,
					block_pos :: pos_integer
             }).

-record(tagTemp, {
				  index :: integer, %% the index of Tag
                  file_id = 0 :: integer,
                  block_pos = 0 :: integer,
                  maxlength = 10*1024*1024 :: integer, %% the max length of the block  
				  length = 0 :: integer %% the current length if the data 
				  }).

-record(state, {
				wFd =0          :: file:io_device() | undefined,
                freespaces = [] :: list, %% the list of #freespace
                basedir = [] :: list, %% the base dir of tempDir 
                errors = []     :: list               
               }).


%% ====================================================================
%% API functions
%% ====================================================================

%% ====================================================================
%% Temp data File
%% Header :
%% <<?VERSION:32/integer, ?ARCHIVE_FILE:8/integer(FILE TYPE), FIdx:32/integer, Unused:64/integer>>
%% <<IndexPos:64/integer, BlockPos:64/integer>>
%% Index area : from top to end
%% <<TagIndex:64/integer, Pos:64/integer, extern:64/integer>> 64 + 64 
%% Block area : from end to top
%% Head
%% HEADER_SIZE = (32 + (64 + 64 + 8 + 64) + (64 + 64 + 8 + 64) + 64) div 8    
%% <<CRC32:32/integer>> 
%% <<FTime:64/integer, FValue:64/integer, FState:8/integer, extern:64/integer>>
%% <<LTime:64/integer, LValue:64/integer, LState:8/integer, extern:64/integer>>
%% <<Length:64/integer>>
%% body
%% <<Data/binary>>
%% CRC(Index)
%% block size is : 32(CRC) + (64 + 8 + 64) + 2*(64 + 64 + 8 + 64) + 64 + Length(Data)
%% ====================================================================


%% ====================================================================
%% Index : the index of the Tag
%% {Time, Value, State} : the record of the data need to save
%% Data : the result that want to be save in the Block
%% 1) was compressed
%% 2) was not compressed
%% ====================================================================

addRecordToTempCall(Index, Time, Value, State, Data) ->
	gen_server:call(?MODULE, {addRecordToTemp, Index, Time, Value, State, Data},  infinity).

addRecordToTemp(Index, Time, Value, State, Data) ->
	gen_server:cast(?MODULE, {addRecordToTemp, Index, Time, Value, State, Data}).
	

%% ====================================================================
%% Internal functions
%% ====================================================================

init([Name, Options]) ->
    process_flag(priority, high),
	BaseDir = ensureBase(),	
	initTables(),
	load_TempfilesFromDB(BaseDir),
	load_tempTagsFromDB(BaseDir),
    {ok, #state{freespaces = load_freespacesFromDB(BaseDir), basedir = BaseDir}}.

start_link(Name, Options) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Name, Options], []).

start() ->
	Name = "",
    start_link(Name, []).

stop() ->   	
   gen_server:cast(?MODULE, stop).

terminate(_Reason, _State) ->
    ok.
    
handle_info(_Info, State) ->
    {noreply, State}.	
 	
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_cast({addRecordToTemp, Index, Time, Value, State, Data}, _State) ->
    FreeSpaces = _State#state.freespaces,
	BaseDir = _State#state.basedir,
    NFreeSpaces = addRecordToTempFile(Index, Time, Value, State, Data, FreeSpaces, BaseDir),     
    {noreply, _State#state{freespaces = NFreeSpaces}};	
	
handle_cast(stop, State) ->
   closeTables(),
   {stop, normal, State}.

handle_call({addRecordToTemp, Index, Time, Value, State, Data}, _From, _State) ->
   FreeSpaces = _State#state.freespaces,
   BaseDir = _State#state.basedir,
   NFreeSpaces = addRecordToTempFile(Index, Time, Value, State, Data, FreeSpaces, BaseDir),
   {reply, ok, _State#state{freespaces = NFreeSpaces}}.

createFile(FIdx, BaseDir) ->
	FileName = BaseDir ++ ?TEMP_PREX ++ integer_to_list(FIdx) ++ ".dat",
	case file:open(FileName, [read, write, raw, binary]) of
	{ok, Fd} -> %%   the | DB version | file type(index archive) | file Index | Unused(64)|
	    file:write(Fd, <<?VERSION:32/integer, ?TEMP_FILE:8/integer, FIdx:32/integer, 0:64/integer>>),
		IndexPos = ?TEMP_HEADER + ?TEMP_POSSEC,
		BlockPos = ?MAX_TEMPSIZE,
	    file:write(Fd, <<IndexPos:64/integer, BlockPos:64/integer>>),
	    file:position(Fd, ?MAX_TEMPSIZE),
		file:truncate(Fd),
		%%file:close(Fd),					
		{ok, FileName, IndexPos, BlockPos};					
    {error, Reason} -> {error, Reason}
	end.

getCRC(TagIndex, Size, CompressType, StartTime, EndTime) ->
	Data = <<TagIndex:64/integer,Size:64/integer,CompressType:8/integer,StartTime:64/integer,EndTime:64/integer>>,
	DataCRC = erlang:adler32(Data),
	DataCRC.

addRecordToTempFile(Index, Time, Value, State, Data, FreeSpaces, BaseDir) ->
	MaxSize = ?MAXSECLEN,
	try
	    case ets:lookup(?TEMPTAGS, Index) of
		[] -> {ok, FId, IndexPos, BlockPos, NFreeSpaces} = getNewBlock(MaxSize, FreeSpaces, BaseDir),
              Length = saveIndexDataToFile(FId, IndexPos, BlockPos, Index, Time, Value, State, Data),
			  TagTemp = #tagTemp{index = Index, block_pos = BlockPos, maxlength = MaxSize, length = Length, file_id = FId},
			  ets:insert(?TEMPTAGS, TagTemp),
			  dets:insert(temptags, TagTemp),
			  NFreeSpaces;
		[#tagTemp{index = Index, file_id = FId, block_pos = BlockPos, length = CurrentLen}] ->
			  DLength = size(Data) + CurrentLen,
			  AvaLeng = ?MAXSECLEN - ?HEADER_SIZE,
			  if DLength =< AvaLeng -> 
		         RLength = saveDataToFile(FId, BlockPos, CurrentLen, Index, Time, Value, State, Data),
				 TagTemp = #tagTemp{index = Index, file_id = FId, block_pos = BlockPos, length = RLength},
				 ets:insert(?TEMPTAGS, TagTemp),
				 dets:insert(temptags, TagTemp);
			  true ->
				 case getBlockData(FId, BlockPos, CurrentLen) of
                 {FTime, FValue, FState, BData} -> 
		           TData = <<BData/binary, Data/binary>>,
			        {RDatLeng, RData} = processBlock(FTime, FValue, FState, TData),
				    tranDatatoAcrch(Index, FTime, Time, RDatLeng, RData);
					_ ->
					  ok
				 end,
				 TagTemp = #tagTemp{index = Index, file_id = FId, block_pos = BlockPos, length = 0},
				 ets:insert(?TEMPTAGS, TagTemp),
				 dets:insert(temptags, TagTemp)
			  end,	  
			  FreeSpaces
		 end
    catch
	    error:Reason ->
	        io:fwrite("Error reason: ~p~n", [Reason]),
			FreeSpaces;
	    throw:Reason ->
	        io:fwrite("Throw reason: ~p~n", [Reason]),
            FreeSpaces;
	    exit:Reason ->
	        io:fwrite("Exit reason: ~p~n", [Reason])

	end.
			   
getNewBlock(Size, FreeSpaces, BaseDir) ->
	TIndexSize = ?INDEX_SIZE,
	TBlockSize = ?HEADER_SIZE + Size,
	TSize = TIndexSize + TBlockSize,	
	case findnearSize(TSize, FreeSpaces) of
	{ok, #freespace{file_id = File_Id, free_size = Free_size, index_pos = Index_pos, block_pos = Block_pos} = FSpace} ->
        DSpaces = lists:delete(FSpace, FreeSpaces),
        dets:delete_object(tempfreespace, FSpace),
		NFree_size = Free_size - TSize,
		if NFree_size >= ?MINBLOCK_SIZE ->
		   ISpace = FSpace#freespace{free_size = NFree_size, index_pos = Index_pos + TIndexSize, block_pos = Block_pos - TBlockSize},	   
		   dets:insert(tempfreespace, ISpace),
		   RSpaces = lists:sort(fun(X,Y) -> {X#freespace.free_size, X#freespace.file_id} < {Y#freespace.free_size, Y#freespace.file_id} end, lists:append([ISpace], DSpaces)),
		   {ok, File_Id, Index_pos, Block_pos, RSpaces};
		true ->   
		   {ok, File_Id, Index_pos, Block_pos, DSpaces}
		end;
     {false} ->
		FIdx = intelDB_TagBase:getTempAbsSeq(),
		FId  = intelDB_TagBase:getTempRelSeq(),
		{ok, FileName, Index_pos, Block_pos} = createFile(FIdx, BaseDir),
		saveIndexFile(FIdx, FId, FileName),
		Free_Size = Block_pos - Index_pos,
		if Free_Size >= TSize ->
			NSpace = #freespace{free_size = Free_Size - TSize, file_id = FId, index_pos = Index_pos + TIndexSize, block_pos = Block_pos - TBlockSize},
			dets:insert(tempfreespace, NSpace),
			RSpaces = lists:sort(fun(X,Y) -> {X#freespace.free_size, X#freespace.file_id} < {Y#freespace.free_size, Y#freespace.file_id} end, lists:append([NSpace], FreeSpaces)),
		    {ok, FId, Index_pos, Block_pos, RSpaces};
		true ->
			{error, newFileSmall}
		end	
    end.
			
findnearSize(Size, []) ->
	{false};

findnearSize(Size, [Space | Rest]) ->
	if Space#freespace.free_size >= Size ->
	   {ok, Space};
	   true ->
		   findnearSize(Size, Rest)
	end.

%% <<CRC32:32/integer>> 
%% <<FTime:64/integer, FValue:64/integer, FState:8/integer, extern:64/integer>>
%% <<LTime:64/integer, LValue:64/integer, LState:8/integer, extern:64/integer>>
%% <<Length:64/integer>>
%% <<Data/binary>>

saveDataToFile(FId, Pos, CurrLen, Index, Time, Value, State, Data) ->
    {ok, Fd} = getOpenFile(FId),
	DLen = size(Data),
	if CurrLen == 0 -> 
	   DataCRC = erlang:adler32(<<FId:32/integer, Index:64/integer, Pos:64/integer>>),
	   Length = DLen,
	   file:position(Fd, Pos),
	   file:write(Fd, <<DataCRC:32/integer, Time:64/integer, Value:64/integer, State:8/integer, 0:64/integer,
                        Time:64/integer, Value:64/integer, State:8/integer, 0:64/integer, Length:64/integer, Data/binary>>),
	   Length;
     true ->
       StartPos = Pos +  ?FIRREC_SIZE,
       Length = CurrLen + DLen,
	   file:position(Fd, StartPos),
       file:write(Fd, <<Time:64/integer, Value:64/integer, State:8/integer, 0:64/integer, Length:64/integer>>),
	   NStartPos = Pos + ?HEADER_SIZE + CurrLen,
       file:position(Fd, NStartPos),
       file:write(Fd, <<Data/binary>>),
       Length
     end.
           
saveIndexDataToFile(FId, IndexPos, BlockPos, Index, Time, Value, State, Data) ->
	case getOpenFile(FId) of
	{ok, Fd}  ->
		Length = size(Data),
		file:position(Fd, BlockPos),
		DataCRC = erlang:adler32(<<FId:32/integer, Index:64/integer, BlockPos:64/integer>>),
	    file:write(Fd, <<DataCRC:32/integer,
						  Time : 64/integer, Value : 64/integer, State : 8/integer, 
						  Time : 64/integer, Value : 64/integer, State : 8/integer,
						  Length : 64/integer,
						  Data/binary  
						  >>),	
		Length;
	{error} -> 
  	   {error}
	end.

getBlockData(FId, BlockPos, CurrentLen) ->
	{ok, Fd} = getOpenFile(FId),
	%%Pos = BlockPos + ?HEADER_SIZE - 64,
    Size = ?HEADER_SIZE - 8,    
	file:position(Fd, BlockPos),
	{ok, HData} = file:read(Fd, Size),
    <<CRC32:32/integer, FTime:64/integer, FValue:64/integer, FState:8/integer, FExtern:64/integer, LTime:64/integer, LValue:64/integer, LState:8/integer, LExtern:64/integer>> = HData,	
	file:write(Fd, <<0:64/integer>>),
	{ok, BData} = file:read(Fd, CurrentLen),
    {FTime, FValue, FState, BData}.

initTables() ->
	ets:new(?OPEND_FILE,  [private, ordered_set, named_table, {keypos, 1}, {read_concurrency, true}, {write_concurrency, true}]), 
	ets:new(?TEMPTAGS, [private, ordered_set, named_table, {keypos, #tagTemp.index}, {read_concurrency, true}, {write_concurrency, true}]),
	ets:new(?MEMTEMPFILES, [private, ordered_set, named_table, {keypos, #tempFiles.file_id}, {read_concurrency, true}, {write_concurrency, true}]).
           
processBlock(FTime, FValue, FState, Data) ->
    %%RData = <<FTime : 64/integer, FValue : 64/integer, FState : 8/integer, Data/binary>>,
    RData = zlib:zip(<<FTime : 64/integer, FValue : 64/integer, FState : 8/integer, Data/binary>>),	
	Length = size(RData),
	{Length, RData}.
	
tranDatatoAcrchCall(Index, StartTime, EndTime, Leng, Data) ->
	CompressType = 1,
	{FId, NBlockPos, NStartTime, NEndTime} = intelDB_ArchManager:addArchBlock(Index, Leng, CompressType, StartTime, EndTime, binary:copy(Data)),
	intelDB_Index:addArchBlock(Index, {trunc(StartTime), {FId, NBlockPos}}),
    ok.

tranDatatoAcrch(Index, StartTime, EndTime, Leng, Data) ->
	CompressType = 1,
	intelDB_ArchManager:addArchBlock(Index, Leng, CompressType, StartTime, EndTime, binary:copy(Data)),
    ok.

getOpenFile(FileId) ->
	case ets:lookup(?OPEND_FILE, FileId) of		
	[{File_Id, Fd, LastTime}] -> 
	  ets:update_element(?OPEND_FILE, File_Id,{3, intelDB_util:stamptomsecs(now())}), 
	  {ok, Fd};
	[] -> 
		case getFilePathById(FileId) of
		{notexist} -> {error};
		FilePath ->
		     case file:open(FilePath, [read, write, raw, binary, {delayed_write, ?DEF_BUFFERSIZE, ?DEF_DELETETIME}]) of
		     {ok, Fd} -> Count = ets:select_count(?OPEND_FILE, [{{'_',  '_',  '_'},[],[true]}]),
		         ets:insert_new(?OPEND_FILE, {FileId, Fd, intelDB_util:stamptomsecs(now())}),		
				 if Count >= ?MAXHANDLE ->
				   {File_Id, DFd, MinTime} = ets:foldl(fun({PFileId, PFd, PLastTime}, {RFileId, RFd ,Min}) ->			
										if PLastTime < Min ->
										   {PFileId, PFd, PLastTime};
										true ->
										   {RFileId, RFd, Min}			
										end
						             end, {-1, {}, ?MAXINTEGER}, ?OPEND_FILE),
				   file:close(DFd),
				   ets:delete(?OPEND_FILE, File_Id),
                   {ok, Fd};
				 true ->
                     {ok, Fd}
				 end;
			{error, Reason} -> {error}
			end
		end
	end.

closeFiles() ->
	ets:foldl(fun({FileId, Fd, LastTime}, DotCare) ->
			  file:close(Fd),
			  DotCare
			  end, notused, ?OPEND_FILE).


getFilePathById(FileId) ->
	case ets:lookup(?MEMTEMPFILES, FileId) of
	[] -> {notexist};
	[#tempFiles{path = FilePath}] ->
		FilePath
	end.

saveIndexFile(FIdx, FId, FileName) ->
	New = #tempFiles{file_idx = FIdx, file_id = FId, path = FileName},
	ets:insert_new(?MEMTEMPFILES, New),
	dets:insert_new(tempfileset, New).

load_tempTagsFromDB(BaseDir) ->
	TempTags = BaseDir ++ ?TAGS_PREX ++ ".dat",
	dets:open_file(temptags, [{file, TempTags}, {type, set},  {keypos,  #tagTemp.index}]),
	ets:from_dets(?TEMPTAGS, temptags).

load_freespacesFromDB(BaseDir) ->
	FileName = BaseDir ++ ?FREE_PREX ++ ".dat", 
	dets:open_file(tempfreespace, [{file, FileName}, {type, bag}, {auto_save, 10}]),
    qlc:eval( qlc:q( [ X || X <- dets:table(tempfreespace)])).

load_TempfilesFromDB(BaseDir) ->
	 FileName = BaseDir ++ ?TEMP_FILES ++ ".dat",
	 dets:open_file(tempfileset, [{file, FileName}, {type, set}, {auto_save, 10}, {keypos, #tempFiles.file_id}]),
     ets:from_dets(?MEMTEMPFILES, tempfileset).

ensureBase() ->
	BaseDir = getBaseDir(),
	case intelDB_util:ensureDir(BaseDir, ?DEF_TEMPDIR) of
	 {ok, NewDir} -> NewDir;
	 {error, Reason} ->
	    %% logger the error 'can not create folder for ' + NewDir.
	    stop() 	 
	end.	 
		
getBaseDir() ->
	case application:get_env(intelDB, tempBaseDir) of
		{ok, TempBaseDir} ->
		 TempBaseDir;
		undefined ->
		 ?DEF_TEMPDIR
	end.

closeTables() ->
   closeFiles(),
   dets:close(tempfileset), 
   dets:close(temptags),
   dets:close(tempfreespace).	

%%===========================================================================
%%  test the function addArchBlock
%%
%%===========================================================================

addRealTimeData(Index, N) ->
   TagIndex = Index,
   CompressType = 1,
   Time = intelDB_util:stamptomsecs(now()),
   
   
	io:format("~w    ~w ~n", [N, Time]),
   
   
   Value= 3000,
   State = 0,
   Data = <<12,44,33,555,333,2222>>,
   addRecordToTemp(TagIndex, Time, Value, State, Data),
   if (N == 0) ->
       ok;
   true ->
	     timer:sleep(1),
       addRealTimeData(Index, N - 1)
   end.

startIntelDB() ->
   intelDB_BuffManager:start(),
   intelDB_ArchManager:start(),
   intelDB_Index:start(),
   intelDB_TempManager:start().
	

test() ->
   intelDB_ArchManager:start(),
   intelDB_Index:start(),
   intelDB_TempManager:start(),
   TagId = 120,
   addRealTimeData(TagId, 60000).   

testStop() ->
   intelDB_ArchManager:stop(),
   intelDB_Index:stop(),
   intelDB_TempManager:stop().
