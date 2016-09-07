-module(intelDB_IndexManager).
-author('Jie-Liu').

-behaviour(gen_server).
-compile(export_all).

-include_lib("stdlib/include/qlc.hrl").

-define(DEF_TIMEOUT, 30000).
-define(DEF_TAGCOUNT, 120000).
-define(DEF_BUFFERSIZE,10240).
-define(DEF_DELETETIME, 2000).
-define(MAX_SUBNODE, 128).
-define(DEF_OUTFILE, "C:/1/data/indx/indexs.dat").
-define(DEF_IDXFOLDER, "C:/1/data/indx/").
-define(MAX_INDEXFILE, 10*1024*1024).
-define(VERSION, 1000000).
-define(INDEX_FILE, 10).
-define(FREE_SPACE, "c:/1/data/indx/fsapces.dat").
-define(TAG_HEADERS, "C:/1/data/indx/headers.dat").
-define(OPEND_FILE,indexOpenfiles).
-define(IDX_FILES,indexfiles).
-define(MAXHANDLE, 20).
-define(MAXINTEGER, 16#FFFFFFFFFFFFFFFF).
-define(INDEX_RELSQU, 0).
-define(INDEX_ABSSQU, 1).
-define(MEMTAGHEADERS, memTagHeader).
-define(HEADERS, "c:/1/data/indx/headers.dat").
-define(MEMINDEXFILES, memIndexFiles).
-define(INDEXSEQUENCE, indexsequence).
-define(INDSEQ, "c:/1/data/indx/sequence.dat").
-define(INDEX_FILES, "c:/1/data/indx/fileidexs.dat").
-define(MAXINT32, 16#FFFFFFFF).
-define(MAXINT64, 16#FFFFFFFFFFFFFFFF).

%=========================================================
% pos_point size = 32 + 64
%=========================================================

-record(pos_point, {file_id :: integer,
					pos :: integer
						}).

%=========================================================
% time_sec size = 64 + 64
%=========================================================
-record(time_sec, { start_time = 0 :: float,
					end_time = ?MAXINT64 :: integer
					}).


%=========================================================
% node_point size = 32 + 64 + 64 + 64
%=========================================================

-record(node_point, {
					 time :: #time_sec{}, pos :: #pos_point{}
					 }).


-record(idxheader, {tagindex = 0 :: unsigned,
                     root :: #node_point{}, left :: #node_point{}, right :: #node_point{}
					}).

-record(state, {tagheaders      :: array | undefined,    %% array of tag headers
                min_time  = 0   :: float,  %% min time 
				max_time  = 0   :: float,  %% max time                   				
                tag_count = 0   :: integer,  %% the count of tag = max of index + 1
				fd              :: file:io_device() | undefined, %% exter file for Header
                freespaces = [] :: list, %% the list of #freespace
                errors = []     :: list               
               }).

-record(fileset, {               
                  file_idx = 0 :: integer, %% the file_idx cannot be changed in life time.
                  file_id=0 :: integer,  %% the file_id can be changed.
                  path :: list
				 }).

-record(freespace, {file_id   :: integer,  %% the file_id can be changed
					free_size :: integer,
					head_pos  :: pos_integer
             }).

-record(sequence, {
				 id = 0 :: integer,   %% the Id of squence 0 : Index File;
                 value = 0 :: integer %% the squence value
				 } ).


-define(NODESIZE, ((32 + 64) * 3 + 32 + 32 + (64 + 64) + ((64 + 64) + (32 + 64)) * ?MAX_SUBNODE) div 8).

-record(idex_node, {pt_node :: #pos_point{},  %% 32 + 64  parent node
					lb_node :: #pos_point{},  %% 32 + 64  left brother node
					rb_node :: #pos_point{},  %% 32 + 64  right brother node
					subnum = 0 :: integer,  %% 32       sub node number
                    level  = 0 :: integer,  %% 32 node level, the leaf is 0
					timesec = #time_sec{},  %% 64 + 64  {min time and max time}
					subnodes = [] :: list   %% array[0..MAX_SUBNODE]	subnodes[0] = #node_point, %% ((64 + 64) + (32 + 64)) * ?MAX_SUBNODE						
            }).

%%=============================================================
%%  PUBLIC API
%%=============================================================

 start_link(Name, Options) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Name, Options], []).

 start() ->
    Name = ?DEF_OUTFILE,
    start_link(Name, []).

 stop() ->   	
   gen_server:cast(?MODULE, stop).

 addArchBlock(Index, Point) ->
   gen_server:call(?MODULE, {addArchBlock, Index, Point},  infinity).

getAllItems(TagIndex) ->
   gen_server:call(?MODULE, {getAllItems, TagIndex},  infinity).
	
	
init() ->
	initTables(),
	load_indexfilesFromDB(),
    load_tagheahersFromDB(),
	load_SequenceFromDB(),
    {ok, #state{freespaces = load_freespacesFromDB(), tag_count = ?DEF_TAGCOUNT}}.
		
init([Name, Options]) ->
	initTables(),
	load_indexfilesFromDB(),
    load_tagheahersFromDB(),
	load_SequenceFromDB(),
    {ok, #state{freespaces = load_freespacesFromDB(), tag_count = ?DEF_TAGCOUNT}}.

load_tagheahersFromDB() ->
	IsDetsFile = dets:is_dets_file(?HEADERS) =:= true,
    if IsDetsFile ->
	   dets:open_file(idxheader, [{file, ?HEADERS}, {type, set},  {keypos, #idxheader.tagindex}]),
	   ets:from_dets(memTagHeader, idxheader);
	true ->
	   initTagHeader(),	
	   dets:open_file(idxheader, [{file, ?HEADERS}, {type, set},  {keypos, #idxheader.tagindex}]),
	   ets:from_dets(memTagHeader, idxheader)
	end.

load_freespacesFromDB() ->
	 dets:open_file(indexfreespace, [{file, ?FREE_SPACE}, {type, bag}, {auto_save, 10}, {keypos, #freespace.free_size}]),
     qlc:eval( qlc:q( [ X || X <- dets:table(indexfreespace)] )).

load_indexfilesFromDB() ->
	 dets:open_file(indexfileset, [{file, ?INDEX_FILES}, {type, set}, {auto_save, 10}, {keypos, #fileset.file_id}]),
     ets:from_dets(?MEMINDEXFILES, indexfileset).

load_SequenceFromDB() ->
	 dets:open_file(indexsequence, [{file, ?INDSEQ}, {type, set}, {auto_save, 10}, {keypos, #sequence.id}]),
     ets:from_dets(?INDEXSEQUENCE, indexsequence).


%%=============================================================
%% Handle case  
%%=============================================================

terminate(_Reason, _State) ->
    ok.
    
handle_info(_Info, State) ->
    {noreply, State}.	
 	
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_cast(stop, State) ->
   closeTables(),
   {stop, normal, State}.

%%=============================================================
%% init System 
%%=============================================================

initTables() ->
	ets:new(?OPEND_FILE,  [set, named_table, {keypos, 1}]), 	
	ets:new(?MEMINDEXFILES, [set, named_table, {keypos, 2}]),		
	ets:new(?INDEXSEQUENCE, [set, named_table, {keypos, 1}]), 
 	ets:new(?MEMTAGHEADERS, [ordered_set, named_table, {keypos, 1}]).

%%=============================================================
%% Free index space Mamager 
%%=============================================================

closeTables() ->
   closeFiles(),	
   dets:close(indexsequence),
   dets:close(idxheader),
   dets:close(indexfileset),   
   dets:close(indexfreespace).	
	
getSequenct(Id) ->
	case ets:lookup(?INDEXSEQUENCE, Id) of
		[] ->
			New = #sequence{id = Id, value = 0};
		[Res] ->
			New = #sequence{id = Id, value = Res#sequence.value + 1}
	end,
    ets:insert(?INDEXSEQUENCE, New),
	dets:insert(indexsequence, New),
	New#sequence.value.
			  
	
getFileId() ->
	Id = ?INDEX_RELSQU,
    getSequenct(Id).

getFileUniqueId() ->
	Id = ?INDEX_ABSSQU,
    getSequenct(Id).

saveIndexFile(FIdx, FId, FileName) ->
	New = #fileset{file_idx = FIdx, file_id = FId, path = FileName},
	ets:insert_new(?MEMINDEXFILES, New),
	dets:insert_new(indexfileset, New).

createNewFile(FIdx, FId) ->
	FileName = ?DEF_IDXFOLDER ++ "Idx" ++ integer_to_list(FIdx) ++ ".dat",
	case file:open(FileName, [read, write, raw, binary]) of
		{ok, Fd} -> FreeSize = ?MAX_INDEXFILE - (32 + 8 + 32 + 64) div 8, 
					%%   the | DB version | file type(index archive) | file Index | free size  |
			        file:write(Fd, <<?VERSION:32/integer, ?INDEX_FILE:8/integer, FIdx:32/integer, FreeSize:64/integer>>),
			        file:position(Fd, ?MAX_INDEXFILE),
					file:truncate(Fd),
					file:close(Fd),
					saveIndexFile(FIdx, FId, FileName),
					{ok, ?MAX_INDEXFILE - (32 + 8 + 32 + 64) div 8, ?MAX_INDEXFILE};					
       {error, Reason} -> {error, Reason}
	end.

%%===============================================================
%% StartTime : is the start time if Item leaf 
%%             SubItem from Acrich file #node_point
%% PosPoint  : mach the Item in PosPoint#Pos_point
%%===============================================================
searchNode(PosPoint, Item) ->
	Node = getNodeFromFile(PosPoint),
	if Node =/= undeifned ->
		%Start = Node#idex_node.timesec#time_sec.start_time,
		%End   = Node#idex_node.timesec#time_sec.end_time,
		Level = Node#idex_node.level,
		if Level > 0  ->
			  case getSubItemFromNode(Node#idex_node.subnodes, Item) of
				  {notfind} -> {notfind};
				  SubItemPos -> searchNode(SubItemPos, Item)
			  end;
		true -> 
			{PosPoint, Node}
		end;
	 true ->
			{PosPoint, undefined}
	end.
		    
getSubItemFromNode(SubNodes, Item) ->
   MidPos = length(SubNodes) div 2 + 1,
   if MidPos == 1 ->
	  [SubItem] = SubNodes,
	  if SubItem#node_point.time#time_sec.start_time =< Item#node_point.time#time_sec.start_time,
		 SubItem#node_point.time#time_sec.end_time > Item#node_point.time#time_sec.start_time ->
		 SubItem;
	  true ->
		  {notfind}
	  end;
   true ->	  
	   [SubItem] = lists:sublist(SubNodes, MidPos, 1),
	   {LeftNodes, RightNodes} = lists:split(MidPos, SubNodes),
	   if SubItem#node_point.time#time_sec.start_time =< Item#node_point.time#time_sec.start_time,
		  SubItem#node_point.time#time_sec.end_time > Item#node_point.time#time_sec.start_time ->
		  SubItem#node_point.pos;
	   SubItem#node_point.time#time_sec.end_time < Item#node_point.time#time_sec.start_time ->
	       getSubItemFromNode(RightNodes, Item);
	   SubItem#node_point.time#time_sec.start_time > Item#node_point.time#time_sec.end_time ->
	       getSubItemFromNode(LeftNodes, Item);
	   true ->
	       {notfind}
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
	
%%======================================================================================
%% add archive record in index file
%% index : tag index.
%% archBlc: the block of archive data of for the Tag index
%% addChildItem : add a Item in the current node.
%% if node.num > maxNum then create new nNode that is brother of the node 
%% then call addChildItem(parent, nNode).
%% Node : the block in index file.
%% Item : {time, pos}, one item of the parent node.
%% Node :: idex_node()
%% Item :: node_point() 
%%====================================================================================== 
getNewNode(FreeSpaces) ->
	FindSize = ?NODESIZE, 
    case findnearSize(FindSize, FreeSpaces) of
	  {ok, FSpace} -> 
            DSpaces = lists:delete(FSpace, FreeSpaces),
	        dets:delete_object(indexfreespace, FSpace),
			RPos = #pos_point{file_id = FSpace#freespace.file_id, pos = FSpace#freespace.head_pos - FindSize},
			if (FSpace#freespace.free_size - FindSize) >= FindSize ->
			   ISpace = #freespace{free_size = FSpace#freespace.free_size - FindSize, file_id = FSpace#freespace.file_id, head_pos = FSpace#freespace.head_pos - FindSize},	
			   dets:insert(indexfreespace, ISpace),
			   RSpaces = lists:sort(fun(X,Y) -> {X#freespace.free_size, X#freespace.file_id} < {Y#freespace.free_size, Y#freespace.file_id} end, lists:append([ISpace], DSpaces)),
			   {ok, RPos, RSpaces};
			true ->   
			   {ok, RPos, DSpaces}
			end;
      {false} ->
			FIdx = getFileUniqueId(),
			FId  = getFileId(),
			{ok, FreeSize, Pos} = createNewFile(FIdx, FId),
			NSpace = #freespace{free_size = FreeSize - FindSize, file_id = FId, head_pos = Pos - FindSize},
			dets:insert(indexfreespace, NSpace),
			RSpaces = lists:sort(fun(X,Y) -> {X#freespace.free_size, X#freespace.file_id} < {Y#freespace.free_size, Y#freespace.file_id} end, lists:append([NSpace], FreeSpaces)),
		    {ok, #pos_point{file_id = FId, pos = Pos - FindSize}, RSpaces}	   
    end.


openFile(FileId) ->
	case ets:lookup(?OPEND_FILE, FileId) of
		
		[{File_Id, Fd, LastTime}] -> 
			ets:update_element(?OPEND_FILE, File_Id,{3, intelDB_RTBase:stamptomsecs(now())}), 
			{ok, Fd};
		[] -> 
			case getFilePathById(FileId) of
				 {notexist} -> {error};
				 FilePath ->
			        case file:open(FilePath, [read, write, raw, binary, {delayed_write, ?DEF_BUFFERSIZE, ?DEF_DELETETIME}]) of
				         {ok, Fd} -> Count = ets:select_count(?OPEND_FILE, [{{'_',  '_',  '_'},[],[true]}]),
							         ets:insert_new(?OPEND_FILE, {FileId, Fd, intelDB_RTBase:stamptomsecs(now())}),		
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
						{error, Reason} -> {error, Reason}
					  end
			end
	end.


closeFiles() ->
	ets:foldl(fun({FileId, Fd, LastTime}, DotCare) ->
			  file:close(Fd),
			  DotCare
			  end, notused, ?OPEND_FILE).


getFilePathById(FileId) ->
	case ets:lookup(?MEMINDEXFILES, FileId) of
		[] -> {notexist};
		[#fileset{file_idx = File_Idx, file_id = File_Id, path = FilePath}] ->
		FilePath
	end.

saveHeaderByIndex(Header) ->  
   ets:insert(memTagHeader, Header),
   dets:insert(idxheader, Header).


saveHeaderByIndex(TagIndex, LNodePoint, RootNodePoint, RNodePoint) ->  
   Header = #idxheader{tagindex = TagIndex, root = RootNodePoint, right = RNodePoint, left = LNodePoint},
   ets:insert(memTagHeader, Header),
   dets:insert(idxheader, Header).


%%===========================================================================
%% Add a subItem in {Pos,Node}
%%===========================================================================
%%init if Root is nil then RootNode=RightNode=LeftNode=getNewNode
%%===========================================================================
%% Level is the level of Node
%% return {ok, Pos, State} pos: the which node SubItem inserted
%%===========================================================================
 addChildItem(TagIdx, Level, Pos, Node, SubItem, State) ->
   FreeSpaces = State#state.freespaces,
   if Node == undefined ->
      SubItems = array:new(?MAX_SUBNODE),	   
      {ok, NPos, NFreeSpaces} = getNewNode(FreeSpaces),
      NNode = #idex_node{subnum = 1, level = Level, subnodes = array:set(0, SubItem, SubItems), timesec = SubItem#node_point.time, lb_node = NPos, rb_node = NPos},
      TimeSec = #time_sec{start_time = SubItem#node_point.time#time_sec.start_time},	  
      RootPoint = #node_point{time = TimeSec, pos = NPos},
      saveHeaderByIndex(#idxheader{tagindex = TagIdx, root = RootPoint, right = RootPoint, left = RootPoint}),
	  saveNodeToFile(NPos, NNode),
      NState = State#state{freespaces = NFreeSpaces},
      {ok, NPos, NState};
   Node#idex_node.subnum < ?MAX_SUBNODE ->
      Num = Node#idex_node.subnum, 
	  TimeSec = #time_sec{start_time = Node#idex_node.timesec#time_sec.start_time, end_time = SubItem#node_point.time#time_sec.end_time},
      NNode = Node#idex_node{subnum = Num + 1, subnodes = array:set(Num, SubItem, Node#idex_node.subnodes), timesec = TimeSec},
      saveNodeToFile(Pos, NNode),
      {ok, Pos, State};  
   true ->
      ParentPos = Node#idex_node.pt_node,  
      if ParentPos =/= undefined ->   
		  ParentNode = getNodeFromFile(ParentPos),
	      {ok, BrotherPos, NFreeSpaces} = getNewNode(FreeSpaces),
		  TimeSec  = #time_sec{start_time = SubItem#node_point.time#time_sec.start_time},          
		  NSubItem = #node_point{time = TimeSec, pos = BrotherPos},
	      PState = State#state{freespaces = NFreeSpaces},
	      {ok, NParentPos, NState} = addChildItem(TagIdx, Level + 1, ParentPos, ParentNode, NSubItem, PState),
		  
		  LNode = Node#idex_node{rb_node = BrotherPos},
	      saveNodeToFile(Pos, LNode),
		  		  
	      BrotherNode = #idex_node{subnum = 1, level = Level,  subnodes = array:set(0, SubItem, array:new(?MAX_SUBNODE)), timesec = TimeSec, lb_node = Pos, pt_node = NParentPos},
	      saveNodeToFile(BrotherPos, BrotherNode),

		  if Level == 0 ->
		     [Header] = ets:lookup(?MEMTAGHEADERS, TagIdx),				 
		     saveHeaderByIndex(Header#idxheader{right = NSubItem}),	
	         {ok, BrotherPos, NState};			 
		  true ->
	         {ok, BrotherPos, NState}
		  end;             
      true ->
          TimeSec = Node#idex_node.timesec,
          LSubItem = #node_point{time = TimeSec, pos = Pos},
	      NSubItems = array:set(0, LSubItem, array:new(?MAX_SUBNODE)),
		  % new parent Pos
          {ok, PPos, PFreeSpaces} = getNewNode(FreeSpaces),

          % new Right Node 
	      {ok, RPos, RFreeSpaces} = getNewNode(PFreeSpaces),
		  RTimeSec = #time_sec{start_time = SubItem#node_point.time#time_sec.start_time},
          RNode = #idex_node{level = Node#idex_node.level, subnum = 1, lb_node = Pos, pt_node = PPos, subnodes = array:set(0, SubItem, array:new(?MAX_SUBNODE)), timesec = RTimeSec},
          RSubItem = #node_point{time = RTimeSec, pos = RPos},
          PSubItems = array:set(1, RSubItem, NSubItems),
          
          % update Left Node
          LNode = Node#idex_node{rb_node = RPos, pt_node = PPos},

          % new parent Node
          PTimeSec = #time_sec{start_time = Node#idex_node.timesec#time_sec.start_time},
          PSubItem = #node_point{time = PTimeSec, pos = PPos},
          PNode = #idex_node{ level = Level + 1, subnum = 2, timesec = PTimeSec, subnodes = PSubItems},
          
          %save left Node
          saveNodeToFile(Pos, LNode),
          %save right node
          saveNodeToFile(RPos, RNode),
          %save parent node
          saveNodeToFile(PPos, PNode),
          NState = State#state{freespaces = RFreeSpaces},
		  if Level == 0 ->
             saveHeaderByIndex(#idxheader{tagindex = TagIdx, root = PSubItem, right = RSubItem, left = LSubItem});
		  true ->
		     [Header] = ets:lookup(?MEMTAGHEADERS, TagIdx),				 	 
		     saveHeaderByIndex(Header#idxheader{root = PSubItem})			  
		  end,
          {ok, RPos, NState}         
      end
   end.


addBlock(Index, Point, State) ->
   case ets:lookup(?MEMTAGHEADERS, Index) of
	   [] ->
           {ok, NPos, NState} = addChildItem(Index, 0, undefined, undefined, Point, State);
	   [Header] ->    
		   Root  = Header#idxheader.root,
		   Right = Header#idxheader.right,
		   Left  = Header#idxheader.left,
		   if Root =/= undefined ->
			   PStartTime = Point#node_point.time#time_sec.start_time,
			   PEndTime   = Point#node_point.time#time_sec.end_time,
			   RStartTime = Right#node_point.time#time_sec.start_time,
			   REndTime   = Right#node_point.time#time_sec.end_time,
			   LStartTime = Left#node_point.time#time_sec.start_time,
			   LEndTime   = Left#node_point.time#time_sec.end_time,
			   
			   if PStartTime > RStartTime ->
			      RNode = getNodeFromFile(Right#node_point.pos),	   
			      {ok, NPos, NState} = addChildItem(Index, 0, Right#node_point.pos, RNode, Point, State);
			   PEndTime < LStartTime ->
			      LNode = getNodeFromFile(Left#node_point.pos),	   
			      {ok, NPos, NState} = addChildItem(Index, 0, Left#node_point.pos, LNode, Point, State);
			   true ->
				  {SPos, SNode} = searchNode(Root#node_point.pos, Point), 
			      {ok, NPos, NState} = addChildItem(Index, 0, SPos, SNode, Point, State)
			   end;
			 true ->
			      {ok, NPos, NState} = addChildItem(Index, 0, undefined, undefined, Point, State)
		     end
   end.
	
getNodeFromFile(PosPoint) ->
   File_Id = PosPoint#pos_point.file_id,
   Pos = PosPoint#pos_point.pos,
   {ok, Fd}  = openFile(File_Id),
   Size = ?NODESIZE,
   file:position(Fd, Pos),
   case file:read(Fd, Size) of
      {ok, Data} ->
           Node = decodeIndexNode(Data);
      {error, Reason} ->
           Node = undefined;
      eof -> 
           Node = undefined
    end,
    Node.

saveNodeToFile(Pos, Node) ->
	     File_Id  = Pos#pos_point.file_id,
		 File_Pos = Pos#pos_point.pos,
		 {ok, Fd} = openFile(File_Id),
		 Data = encodeIndexNode(Node),
         file:position(Fd, File_Pos),
         case file:write(Fd, <<Data/binary>>) of
             ok -> ok;
             {error, Reason} -> {error, Reason}
		end.	 


%==========================================================================
%  encode_index_node
%==========================================================================

posToBin(Pos, Bin) ->
	if Pos =/= undefined ->
	   File_id = Pos#pos_point.file_id,
	   FPos = Pos#pos_point.pos;
	true -> 
	   File_id = ?MAXINT32,
	   FPos = ?MAXINT64
	end,
	<<Bin/binary, File_id:32/integer, FPos:64/integer>>.

binToPos(Bin) ->
	   <<File_id:32/integer, Pos:64/integer, ResBin/binary>> = Bin, 
	   if ((File_id == ?MAXINT32) and (Pos == ?MAXINT64)) ->
		  RPos = undefined;
	   true ->
	      RPos = #pos_point{file_id = File_id, pos = Pos}
	   end,
       {RPos, ResBin}.
	   
timesecToBin(TimeSec, Bin) ->
	   if TimeSec =/= undefined ->
	      Start_Time = TimeSec#time_sec.start_time,
	      End_Time  = TimeSec#time_sec.end_time;
       true ->
          Start_Time = ?MAXINT32,
          End_Time  = ?MAXINT64
       end,
	   <<Bin/binary, Start_Time:64/float, End_Time:64/float>>.

binToTimesec(Bin) ->
	   <<Start_Time:64/float, End_Time:64/float, ResBin/binary>> = Bin,
	   if ((Start_Time == ?MAXINT32) and (End_Time == ?MAXINT64)) ->
		  Time = undefined;
	   true ->
		  Time = #time_sec{start_time = Start_Time, end_time = End_Time}
	   end,
	   {Time, ResBin}.
	   
binToSubItems(N, N, SubItems, Bin) ->
	   {SubItems, Bin};

binToSubItems(S, N, SubItems, Bin) ->
       {Time, TBin} = binToTimesec(Bin),
	   {Pos, PBin} = binToPos(TBin),
	   Item = #node_point{time = Time, pos = Pos},
	   NSubItems = array:set(S, Item, SubItems),
       binToSubItems(S + 1, N, NSubItems, PBin).
	   
subItemsToBin(N, N, SubItems, Bin) ->
       Bin;
subItemsToBin(S, N, SubItems, Bin) ->
       Item = array:get(S, SubItems),
	   if Item =/= undefined ->
		  Time = Item#node_point.time,
		  Pos  = Item#node_point.pos,
		  Item#node_point{time = Time, pos = Pos},
		  TS_Data = timesecToBin(Time, Bin),
		  PS_Data = posToBin(Pos, TS_Data),
		  subItemsToBin(S + 1, N, SubItems, PS_Data); 
	   true ->
		  DTime = undefined,
		  DPos  = undefined,
		  DT_Data = timesecToBin(DTime, Bin),
		  DP_Data = posToBin(DPos, DT_Data),
		  subItemsToBin(S + 1, N, SubItems, DP_Data)	 
	   end.	   	   

encodeIndexNode(Node) ->
	   #idex_node{pt_node = Pt_node, lb_node = Lb_node, rb_node = Rb_node, subnum = Subnum, level = Level, timesec = Timesec, subnodes = Subnodes} = Node,
	   Pt_Data = posToBin(Pt_node, <<>>),
	   Lb_Data = posToBin(Lb_node, Pt_Data),
	   Rb_Data = posToBin(Rb_node, Lb_Data),
       
	   Sb_Data = <<Rb_Data/binary, Subnum:32/unsigned, Level:32/unsigned>>,
	   TS_Data = timesecToBin(Timesec, Sb_Data),
	   RT_Data = subItemsToBin(0, ?MAX_SUBNODE, Subnodes, TS_Data),
	   RT_Data.
	
decodeIndexNode(Data) ->
	  {Pt_node, Pt_Data} = binToPos(Data),
	  {Lb_node, Lb_Data} = binToPos(Pt_Data),
	  {Rb_node, Rb_Data} = binToPos(Lb_Data),
	  <<Subnum:32/unsigned, Level:32/unsigned, ResBin/binary>> = Rb_Data,
	  {Time, Tm_Data} = binToTimesec(ResBin),
	  {SubItems, Bin} = binToSubItems(0, ?MAX_SUBNODE, array:new(?MAX_SUBNODE), Tm_Data),
	  Node = #idex_node{pt_node = Pt_node, lb_node = Lb_node, rb_node = Rb_node, subnum = Subnum, level = Level, timesec = Time, subnodes = SubItems},
	  Node. 
	  
%================================================================================================================
%for init when the ?HEADERS file does not exist then create defult file
%================================================================================================================
saveHeader(N) ->
	if N < ?DEF_TAGCOUNT ->
	   Start_time = ?MAXINT64,
	   End_time = ?MAXINT64,
	   File_id = 0,
	   Pos = ?MAXINT64,
	   Lp_node = #node_point{time = #time_sec{start_time = Start_time, end_time = End_time}, pos = #pos_point{file_id = File_id, pos = Pos}},	   
	   Tagheader = #idxheader{tagindex = N},
       ets:insert_new(?MEMTAGHEADERS, Tagheader),
	   saveHeader(N + 1);
	true -> ok
	end.

initTagHeader() ->
    N = 0,
    saveHeader(N),
	dets:open_file(idxheader, [{file, ?HEADERS}, {type, set}, {auto_save, 10}, {keypos, #idxheader.tagindex}]),
	ets:to_dets(?MEMTAGHEADERS, idxheader),
	dets:close(idxheader).

%================================================================================================================



%================================================================================================================
% Handle_call functions
%================================================================================================================
getParentNode1(Pos) ->
	if Pos =/= undefined ->
	   Node = getNodeFromFile(Pos),
	   io:format("~w   ~w~n", [Node#idex_node.level, Node#idex_node.subnum]),
	   
	   
	   
	   if Node#idex_node.pt_node == undefined ->
		   io:format("~w~n", [Node]);
		  
		  
		  
       true ->
		   getParentNode(Node#idex_node.pt_node)
       end;		   
	true ->
	   ok
	end.
	   
getParentNode(Pos) ->
	if Pos =/= undefined ->
	   Node = getNodeFromFile(Pos),
	   io:format("~w   ~w~n", [Node#idex_node.level, Node#idex_node.subnum]),
	   io:format("~w                 ~w~n", [Node#idex_node.timesec#time_sec.start_time, Node#idex_node.timesec#time_sec.end_time]),	   	   
	   getParentNode(Node#idex_node.pt_node);
	true ->
	   ok
	end.
	   



getNextNode(Node) ->
	if Node =/= undefined ->
	   io:format("~w    ~w~n", [Node#idex_node.level, Node#idex_node.subnum]),
	   io:format("~w~n", [Node#idex_node.timesec#time_sec.start_time]),	   
	   if Node#idex_node.rb_node =/= undefined ->
		   RPos = Node#idex_node.rb_node,
		   RNode = getNodeFromFile(RPos),
	   
		   %%================================================================
	       getParentNode(RNode#idex_node.pt_node),	   
		   %%================================================================
	       getNextNode(RNode);
	   true ->
		   ok
	   end;
	true ->
	   ok
	end.




handle_call({getAllItems, Index}, From,State) ->
   [Header] = ets:lookup(?MEMTAGHEADERS, Index),	
   Root  = Header#idxheader.root,
   Left  = Header#idxheader.left,
   if Root =/= undefined ->
	   %%LNode = getNodeFromFile(Left#node_point.pos),
	   %%getNextNode(LNode),
	   Node = getNodeFromFile(Root#node_point.pos), 
	   outNode(Node),
       {reply, ok, State};
	true ->
       {reply, ok, State}
    end;


handle_call({addArchBlock, Index, Point}, From,State) ->
   {ok, NPos, NState} = addBlock(Index, Point, State),
   {reply, {ok, NPos}, NState}.


%================================================================================================================








	





%================================================================================================================
%  For Test fuctions
%================================================================================================================
test1(Num, TagIndex) ->
	if Num >= 0 ->
       testAdd(TagIndex, Num),
	   io:format("~w~n", [Num]),	   
	   test1(Num -1, TagIndex);
	true ->
       ok
	end.



test(N) ->
	if N >= 0 ->
       testAdd(N, N),
	   io:format("~w~n", [N]),	   
	   test(N -1);
	true ->
       ok
	end.

testAdd(TagIndex, N) ->
 Start_time = intelDB_RTBase:stamptomsecs(now()),
 End_time   = intelDB_RTBase:stamptomsecs(now()),
 File_Id = 0, 
 Pos = N,
 Point = #node_point{time = #time_sec{start_time = Start_time, end_time = End_time}, pos = #pos_point{file_id =File_Id, pos = Pos}},
 addArchBlock(TagIndex, Point).




test2(TagIndex) ->
 getAllItems(TagIndex).



 
 
 
outSubNode(Num, Num, Level, SubItems) ->
	ok;

outSubNode(N, Num, Level, SubItems) ->
  	SubPoint = array:get(N, SubItems),
	if Level > 0 ->	    
	   SubNode  = getNodeFromFile(SubPoint#node_point.pos),
	   io:format("~w    ~w    ~w~n", [Level, N, SubNode#idex_node.timesec]),
	   outNode(SubNode);
	true ->
	   io:format("~w    ~w    ~w~n", [Level, N, SubPoint#node_point.time])
    end,	
	outSubNode(N + 1, Num, Level, SubItems).
	
outNode(Node) ->
  Num   = Node#idex_node.subnum,
  Level = Node#idex_node.level,
  SubItems = Node#idex_node.subnodes,
  outSubNode(0, Num, Level, SubItems).



testout(Index) ->
   [Header] = ets:lookup(?MEMTAGHEADERS, Index),	
   Root  = Header#idxheader.root,
   Node = getNodeFromFile(Root#node_point.pos),
   outNode(Node).

%================================================================================================================
