%% @author jliu4
%% @doc @todo Add description to intelDB_ArchManager.

-module(intelDB_ArchManager).
-author('Jie-Liu').

-behaviour(gen_server).
-compile(export_all).

-include_lib("stdlib/include/qlc.hrl").

-define(FILETYPE, 100).
-define(DEF_BUFFERSIZE,1024*40).
-define(DEF_DELETETIME, 10000).
-define(MAXHANDLE, 5).
-define(BLOCKSIZE, 512).
-define(OPEND_FILE,archOpenfiles).
-define(MEMARCHFILES, memArchFiles).

-define(DEF_ARCHDIR, "c:/IntelDB/data/archBase/").
-define(ARCH_PREX, "Arch").
-define(FREE_PREX, "ArchSpace").
-define(ARCH_FILE, "ArchFiles").

-define(INDEX_SIZE, (64 + 64 + 64 + 64) div 8).
-define(HEADER_SIZE, (32 + 64 + 8 + 64) div 8).
-define(ARCH_HEADER, (32 + 8 + 32 + 64) div 8).
-define(ARCH_POSITION, (64 + 64) div 8).
-define(MINBLOCK_SIZE, 24*1024).
-define(MAXFILEBUFF, 10).
-define(MAXINTEGER, 16#FFFFFFFFFFFFFFFF).
-define(GetBlocks(SIZE), ((Size + BLOCKSIZE -1) * BLOCKSIZE)).

-record(archiveFiles, {               
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

-record(state, {
				wFd      :: file:io_device() | undefined,
                freespaces = [] :: list, %% the list of #freespace
                basedir = [] :: list, %% the base dir of archive 
                errors = [] :: list               
               }).

%% ====================================================================
%% API functions
%% ====================================================================

%% ====================================================================
%% Archive data File
%% Header :
%% <<?VERSION:32/integer, ?ARCHIVE_FILE:8/integer(FILE TYPE), FIdx:32/integer, Unused:64/integer>>
%% <<IndexPos:64/integer, BlockPos:64/integer>>
%% Index area : from top to end
%% <<TagIndex:64/integer, StartTime:64/integer, EndTime:64/integer, Pos:64/integer>>
%% Block area : from end to top
%% <<CRC32:32/integer,  Size:64/integer, CompressType:8/integer, extern:64/integer>>
%% <<Data:Size/binary>>
%% for every block size is : 64(size) + 8(comtype) + 64(extern) + size(data)
%% ====================================================================

%% ====================================================================
%% addArchBlock: add a new bolck archive data to archive file
%% TagIndex :: 64 tag index
%% Size :: 64 the size of the Data
%% CompressType :: 8 the type of compress for Data
%% ====================================================================

addArchBlockCall(TagIndex, Size, CompressType, StartTime, EndTime, Data) ->
	{ok,FId, NBlockPos, StartTime, EndTime} = gen_server:call(?MODULE, {addArchBlockCall, TagIndex, Size, CompressType, StartTime, EndTime, Data},  infinity),
	{FId, NBlockPos, StartTime, EndTime}.

addArchBlock(TagIndex, Size, CompressType, StartTime, EndTime, Data) ->
	gen_server:cast(?MODULE, {addArchBlock, TagIndex, Size, CompressType, StartTime, EndTime, Data}).

%% ====================================================================
%% Internal functions
%% ====================================================================

init([Name, Options]) ->
	BaseDir = ensureBase(),
	initTables(BaseDir),
	load_ArchfilesFromDB(BaseDir),
    {ok, #state{freespaces = load_freespacesFromDB(BaseDir), basedir = BaseDir}}.

start_link(Name, Options) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Name, Options], []).

start() ->
    start_link("", []).

stop() ->   	
   gen_server:cast(?MODULE, stop).

terminate(_Reason, _State) ->
    ok.
    
handle_info(_Info, State) ->
    {noreply, State}.	
 	
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_cast({addArchBlock, TagIndex, Size, CompressType, StartTime, EndTime, Data}, State) ->
   FreeSpaces = State#state.freespaces,
   BaseDir = State#state.basedir,
   {ok, NFreeSpaces} = addBlockToArchFile(TagIndex, Size, CompressType, StartTime, EndTime, Data, FreeSpaces, BaseDir),
   {noreply, State#state{freespaces = NFreeSpaces}};	

handle_cast(stop, State) ->
   closeTables(),
   {stop, normal, State}.

initTables(BaseDir) ->
	%% Record of Open_file   {FileId, Fd, stamp}  %%	
	ets:new(?OPEND_FILE,  [set, named_table, {keypos, 1}]), 
	ets:new(?MEMARCHFILES,[set, named_table, {keypos, #archiveFiles.file_id}]).

createFile(FIdx, BaseDir) ->
	FileName = BaseDir ++ ?ARCH_PREX ++ integer_to_list(FIdx) ++ ".dat",
	case file:open(FileName, [read, write, raw, binary]) of
	{ok, Fd} -> %%   the | DB version | file type(index archive) | file Index | Unused(64)|
        Version = intelDB_util:getVersion(),
        file:write(Fd, <<Version:32/integer, ?FILETYPE:8/integer, FIdx:32/integer, 0:64/integer>>),
		IndexPos = ?ARCH_HEADER + ?ARCH_POSITION,
		BlockPos = intelDB_util:getMaxFileSize(),
        file:write(Fd, <<IndexPos:64/integer, BlockPos:64/integer>>),
        file:position(Fd, BlockPos),
		file:truncate(Fd),
		file:close(Fd),					
		{ok, FileName, IndexPos, BlockPos};					
    {error, Reason} -> {error, Reason}
	end.

getCRC(TagIndex, Size, CompressType, StartTime, EndTime) ->
	Data = <<TagIndex:64/integer,Size:64/integer,CompressType:8/integer,StartTime:64/float,EndTime:64/float>>,
	DataCRC = erlang:adler32(Data),
	DataCRC.

addBlockToArchFile(TagIndex, Size, CompressType, StartTime, EndTime, Data, FreeSpaces, BaseDir) ->
    {ok, FId, IndexPos, BlockPos, NFreeSpaces} = getNextBlock(Size, FreeSpaces, BaseDir),
	{ok, WFd} = getOpenFile(FId, BaseDir),
	
	NIndexPos = IndexPos + ?INDEX_SIZE,
	NBlockPos = BlockPos - ?HEADER_SIZE - Size,
	file:position(WFd, ?ARCH_HEADER),
	file:write(WFd, <<NIndexPos:64/integer, NBlockPos:64/integer>>),
    file:position(WFd, IndexPos),
	file:write(WFd, <<TagIndex:64/integer, StartTime:64/float, EndTime:64/float, NBlockPos:64/integer>>),		
    file:position(WFd, NBlockPos),
	CRC32 = getCRC(TagIndex, Size, CompressType, StartTime, EndTime),
    file:write(WFd, <<CRC32:32/integer, Size:64/integer, CompressType:8/integer, 0:64>>),
	file:write(WFd, <<Data/binary>>),
    intelDB_Index:addArchBlock(TagIndex, {trunc(StartTime), {FId, NBlockPos}}),		
	{ok, NFreeSpaces}.


getNextBlock(Size, FreeSpaces, BaseDir) ->
	TIndexSize = ?INDEX_SIZE,
	TBlockSize = ?HEADER_SIZE + Size,
	TSize = TIndexSize + TBlockSize,	
	
	case findnearSize(TSize, FreeSpaces) of
	{ok, #freespace{file_id = File_Id, free_size = Free_size, index_pos = Index_pos, block_pos = Block_pos} = FSpace} ->
        DSpaces = lists:delete(FSpace, FreeSpaces),
        dets:delete_object(archfreespace, FSpace),
		NFree_size = Free_size - TSize,
		if NFree_size >= ?MINBLOCK_SIZE ->
		   ISpace = FSpace#freespace{free_size = NFree_size, index_pos = Index_pos + TIndexSize, block_pos = Block_pos - TBlockSize},	   
		   dets:insert(archfreespace, ISpace),
		   RSpaces = lists:sort(fun(X,Y) -> {X#freespace.free_size, X#freespace.file_id} < {Y#freespace.free_size, Y#freespace.file_id} end, lists:append([ISpace], DSpaces)),
		   {ok, File_Id, Index_pos, Block_pos, RSpaces};
		true ->   
		   {ok, File_Id, Index_pos, Block_pos, DSpaces}
		end;
    {false} ->
		FIdx = intelDB_TagBase:getArchAbsSeq(),
		FId  = intelDB_TagBase:getArchRelSeq(),
		
		{ok, FileName, Index_pos, Block_pos} = createFile(FIdx, BaseDir),
		saveIndexFile(FIdx, FId, FileName),
		Free_Size = Block_pos - Index_pos,
		if Free_Size >= TSize ->
			NSpace = #freespace{free_size = Free_Size - TSize, file_id = FId, index_pos = Index_pos + TIndexSize, block_pos = Block_pos - TBlockSize},
			dets:insert(archfreespace, NSpace),
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

openRegistFile(FileName, FileId ) ->	
    case file:open(FileName, [read, write, raw, binary, {delayed_write, ?DEF_BUFFERSIZE, ?DEF_DELETETIME}]) of
    {ok, Fd} ->
	   Count = ets:select_count(?OPEND_FILE, [{{'_',  '_',  '_'},[],[true]}]),
	   Ret = 
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
	   end,
       ets:insert_new(?OPEND_FILE, {FileId, Fd, intelDB_util:stamptomsecs(now())}),
	   Ret;
	{error, Reason} -> {error, Reason}
	end.	

getOpenFile(FileId, BaseDir) ->
	case ets:lookup(?OPEND_FILE, FileId) of		
	[{File_Id, Fd, LastTime}] -> 
		ets:update_element(?OPEND_FILE, File_Id, {3, intelDB_util:stamptomsecs(now())}), 
		{ok, Fd};
	[] -> 
		case getFilePathById(FileId) of
		{notexist} -> 
		   FIdx = intelDB_TagBase:getArchAbsSeq(), 
		   {ok, FileName, IndexPos, BlockPos} = createFile(FIdx, BaseDir),
		   saveIndexFile(FIdx, FileId, FileName),
		   openRegistFile(FileName, FileId);
		FilePath ->
		   openRegistFile(FilePath, FileId)
        end
	end.

getFilePathById(FileId) ->
	case ets:lookup(?MEMARCHFILES, FileId) of
	[] -> {notexist};
	[#archiveFiles{path = FilePath}] ->
		FilePath
	end.

saveIndexFile(FIdx, FId, FileName) ->
	New = #archiveFiles{file_idx = FIdx, file_id = FId, path = FileName},
	ets:insert_new(?MEMARCHFILES, New),
	dets:insert_new(archOpenfiles, New).

load_ArchfilesFromDB(BaseDir) ->
	 ArchFile = BaseDir  ++ ?ARCH_FILE ++ ".dat",
	 dets:open_file(archOpenfiles, [{file, ArchFile}, {type, set}, {auto_save, 10}, {keypos, #archiveFiles.file_id}]),
     ets:from_dets(?MEMARCHFILES, archOpenfiles).

load_freespacesFromDB(BaseDir) ->
	 ArchSpace =  BaseDir ++ ?FREE_PREX ++ ".dat",
	 dets:open_file(archfreespace, [{file, ArchSpace}, {type, bag}, {auto_save, 10}, {keypos, 2}]),
     qlc:eval( qlc:q( [ X || X <- dets:table(archfreespace)])).

closeTables() ->
   dets:close(archOpenfiles),   
   dets:close(archfreespace).	

ensureBase() ->
	BaseDir = getBaseDir(),
	case intelDB_util:ensureDir(BaseDir, ?DEF_ARCHDIR) of
	 {ok, NewDir} -> NewDir;
	 {error, Reason} ->
	    %% logger the error 'can not create folder for ' + NewDir.
	    stop() 	 
	end.	 
		
getBaseDir() ->
	case application:get_env(intelDB, archBaseDir) of
		{ok, ArchBaseDir} ->
		 ArchBaseDir;
		undefined ->
		 ?DEF_ARCHDIR
	end.
	






%%===========================================================================
%%  test the function addArchBlock
%%
%%===========================================================================
test() ->
   TagIndex =  random:uniform(500),
   Size = 8 * 1024,
   CompressType = 1,
   StartTime = 10000.0,
   EndTime   = 30000.0,
   Data = <<812,44,33,555,333,2222>>,
   addArchBlock(TagIndex, Size, CompressType, StartTime, EndTime, Data),
   timer:sleep(1),   
   test().