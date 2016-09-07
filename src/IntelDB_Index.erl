-module(intelDB_Index).
-author('Jie-Liu').

-behaviour(gen_server).
-compile(export_all).

-include_lib("stdlib/include/qlc.hrl").

-define(MAX_SUBNODE, 64).
-define(DEF_TIMEOUT, 30000).
-define(DEF_TAGCOUNT, 120000).
-define(DEF_BUFFERSIZE,10240).
-define(DEF_DELETETIME, 2000).

-define(DEF_INDXDIR, "c:/IntelDB/data/indxBase/").
-define(INDX_PREX, "Indx").
-define(FILE_PREX, "Files").
-define(HEAD_PREX, "Head").
-define(FREE_PREX, "Space").

-define(MAX_INDEXFILE, 1024*1024*1024).
-define(VERSION, 1000000).
-define(INDEX_FILE, 10).
-define(OPEND_FILE, indexOpenfiles).
-define(IDX_FILES, indexfiles).
-define(MAXHANDLE, 20).
-define(MEMTAGHEADERS, memTagHeader).
-define(MEMINDEXFILES, memIndexFiles).
-define(MAXINTEGER, 16#FFFFFFFFFFFFFFFF).
-define(MAXINT32, 16#FFFFFFFF).
-define(MAXINT64, 16#FFFFFFFFFFFFFFFF).

-define(NODESIZE, (16 + (64 + 64) + 64 + (64 + 64*2) * ?MAX_SUBNODE) div 8).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% term_to_binary
% binary_to_term
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%=========================================================
% pointer size = 32 + 64
%=========================================================

-record(pointer, {file_id = 0 :: integer,
					  pos = 0 :: integer
				  }).

-record(kv, {k = 0 :: integer,
			 value :: #pointer{}}).

-record(header, { index = 0 :: unsigned,
				  pointers = [] :: list  %% #kv = {K, Pointer} % pointers[0] = root, pointers[1] = Leverl[1].rightest.....pointers[n-1] = Leverl[n-1].rightest 
				}).

-record(node, { type  = kv_node,      %% kv_node leaf; kp_node root
                rb_node :: #pointer{},  %% 64 + 64  right brother node pointer
				subnum = 0 :: integer,  %% 32  sub node number
				subnodes = [] :: list   %% array[0..MAX_SUBNODE] subnodes[0] = #kv = {K,Pointer} 						
               }).

%=========================================================
% files manager
%=========================================================

-record(fileset, {               
                  file_idx = 0 :: integer, %% the file_idx cannot be changed in life time.
                  file_id = 0  :: integer, %% the file_id can be changed.
                  path :: list
				 }).

-record(freespace, {file_id   :: integer,   %% the file_id can be changed
					free_size :: integer,
					head_pos  :: pos_integer
                }).

-record(state, {headers      :: array | undefined,    %% array of tag headers
                basedir = [] :: list, %% the base dir of archive 
                freespaces = [] :: list %% the list of #freespace              
               }).

%%=============================================================
%%  PUBLIC API
%%=============================================================

 start_link(Name, Options) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Name, Options], []).

 start() ->
    start_link("", []).

 stop() ->   	
   gen_server:cast(?MODULE, stop).

addArchBlockCall(Index, Pointer) ->
   gen_server:call(?MODULE, {addArchBlockCall, Index, Pointer},  infinity).

addArchBlock(Index, Pointer) ->
   gen_server:cast(?MODULE, {addArchBlock, Index, Pointer}).

getAllItems(Index) ->
   gen_server:call(?MODULE, {getAllItems, Index},  infinity).


searchItem(Index, Key) ->
   gen_server:call(?MODULE, {searchItem, Index, Key},  infinity).

init() ->
	BaseDir = ensureBase(),
	initTables(),
	load_indexfilesFromDB(BaseDir),
    load_tagheahersFromDB(BaseDir),
    {ok, #state{freespaces = load_freespacesFromDB(BaseDir), basedir = BaseDir}}.
		
init([Name, Options]) ->
	BaseDir = ensureBase(),
	initTables(),
	load_indexfilesFromDB(BaseDir),
    load_tagheahersFromDB(BaseDir),
    {ok, #state{freespaces = load_freespacesFromDB(BaseDir), basedir = BaseDir}}.

load_tagheahersFromDB(BaseDir) ->
	HeadFile = BaseDir ++ ?HEAD_PREX ++ ".dat",
	IsDetsFile = dets:is_dets_file(HeadFile) =:= true,
    if IsDetsFile ->
	   dets:open_file(idxheader, [{file, HeadFile}, {type, set},  {keypos, #header.index}]),
	   ets:from_dets(?MEMTAGHEADERS, idxheader);
	true ->
	   initTagHeader(BaseDir),	
	   dets:open_file(idxheader, [{file, HeadFile}, {type, set},  {keypos, #header.index}]),
	   ets:from_dets(?MEMTAGHEADERS, idxheader)
	end.

load_freespacesFromDB(BaseDir) ->
	SpaceFile = BaseDir ++ ?FREE_PREX ++ ".dat",
	 dets:open_file(indexfreespace, [{file, SpaceFile}, {type, bag}, {auto_save, 10}, {keypos, 2}]),
     qlc:eval( qlc:q( [ X || X <- dets:table(indexfreespace)] )).

load_indexfilesFromDB(BaseDir) ->
	 IndexFile = BaseDir ++ ?FILE_PREX ++ ".dat", 
	 dets:open_file(indexfileset, [{file, IndexFile}, {type, set}, {auto_save, 10}, {keypos, #fileset.file_id}]),
     ets:from_dets(?MEMINDEXFILES, indexfileset).

%================================================================================================================
% Handle_call functions
%================================================================================================================
handle_call({getAllItems, Index}, From, State) ->
   {ok, NState} = getBlock(Index, State),
   {reply, {ok}, NState};

handle_call({addArchBlockCall, Index, Point}, From, State) ->
   {ok, NState} = addBlockCall(Index, Point, State),
   {reply, {ok}, NState};

handle_call({searchItem, Index, Key}, From, State) ->
   {ok, KV} = searchItem(Index, Key, State),
   {reply, KV, State}.

%%=============================================================
%% Handle case  
%%=============================================================

terminate(_Reason, _State) ->
    ok.
    
handle_info(_Info, State) ->
    {noreply, State}.	
 	
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_cast({addArchBlock, Index, Point}, State) ->
   {ok, NState} = addBlock(Index, Point, State),
   {noreply, NState};   

handle_cast(stop, State) ->
   closeTables(),
   {stop, normal, State}.

%%=============================================================
%% init System 
%%=============================================================

initTables() ->
	ets:new(?OPEND_FILE,  [set, named_table, {keypos, 1}]), 	
	ets:new(?MEMINDEXFILES, [set, named_table, {keypos, #fileset.file_id}]),		
 	ets:new(?MEMTAGHEADERS, [ordered_set, named_table, {keypos, #header.index}]).

%%=============================================================
%% Free index space Mamager 
%%=============================================================

closeTables() ->
   closeFiles(),	
   dets:close(idxheader),
   dets:close(indexfileset),   
   dets:close(indexfreespace).	
	
closeFiles() ->
	ets:foldl(fun({FileId, Fd, LastTime}, DotCare) ->
			  file:close(Fd),
			  DotCare
			  end, notused, ?OPEND_FILE).

initTagHeader(BaseDir) ->
    N = 0,
	HeadFile = BaseDir ++ ?HEAD_PREX ++ ".dat",
    saveHeader(N),
	dets:open_file(idxheader, [{file, HeadFile}, {type, set}, {auto_save, 10}, {keypos, #header.index}]),
	ets:to_dets(?MEMTAGHEADERS, idxheader),
	dets:close(idxheader).

save_Header(Header) ->  
   ets:insert(?MEMTAGHEADERS, Header),
   dets:insert(idxheader, Header).


findnearSize(Size, []) ->
	{false};

findnearSize(Size, [Space | Rest]) ->	
	if Space#freespace.free_size >= Size ->
	   {ok, Space};
	   true ->
		   findnearSize(Size, Rest)
	end.

saveIndexFile(FIdx, FId, FileName) ->
	New = #fileset{file_idx = FIdx, file_id = FId, path = FileName},
	ets:insert_new(?MEMINDEXFILES, New),
	dets:insert_new(indexfileset, New).

createFile(FIdx, FId, BaseDir) ->
	FileName = BaseDir ++ ?INDX_PREX ++ integer_to_list(FIdx) ++ ".dat",
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

openFile(FileId) ->
	case ets:lookup(?OPEND_FILE, FileId) of		
	[{File_Id, Fd, LastTime}] -> 
		ets:update_element(?OPEND_FILE, File_Id,{3, intelDB_util:stamptomsecs(now())}), 
		{ok, Fd};
	[] -> 
		case getFilePath(FileId) of
		{notexist} -> 
			{error};
		{FilePath} ->
	        case file:open(FilePath, [read, write, raw, binary, {delayed_write, ?DEF_BUFFERSIZE, ?DEF_DELETETIME}]) of
		    {ok, Fd} -> 
				 Count = ets:select_count(?OPEND_FILE, [{{'_',  '_',  '_'},[],[true]}]),
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
			{error, Reason} -> 
			   {error, Reason}
			end
		end
	end.


getFilePath(FileId) ->
	case ets:lookup(?MEMINDEXFILES, FileId) of
		[] -> {notexist};
		[#fileset{path = FilePath}] ->
		{FilePath}
	end.

modify_node(Bt, RootPointer, Action, QueryOutput) ->
    case RootPointer of
    nil ->
        NodeType = kv_node,
        NodeList = [],
		RightPointer = #pointer{};
    _Tuple ->
	    #node{type = NodeType, rb_node = RightPointer, subnodes = NodeList} = get_node(RootPointer)
    end,

    {ok, NewNodeList, QueryOutput2, NewBt} =
    case NodeType of
       kp_node -> modify_kpnode(Bt, NodeList, Action, QueryOutput);
       kv_node -> modify_kvnode(Bt, NodeList, Action, QueryOutput)
    end,
	
    case NewNodeList of
    [] ->  % no nodes remain free the Pointer
        {ok, [], QueryOutput2, NewBt};
    NodeList ->  % nothing changed
        [{Key, _} | _] = NodeList,
        {ok, [{Key, RootPointer}], QueryOutput2, NewBt};
    _Else ->
        {State, ResultList} = write_node(NewBt, RootPointer, RightPointer, NodeType, NewNodeList),
		case (RightPointer == undefined) and (length(ResultList) > 1) of 
		true ->
			NewQueryOutput = lists:keyreplace(RootPointer, #kv.value, QueryOutput2, lists:last(ResultList));			
		false ->
			NewQueryOutput = QueryOutput2
		end,	
        {ok, ResultList, NewQueryOutput, State}
    end.

modify_kpnode(Bt, NodeList, {_, FirstActionKey, _} = Action,  QueryOutput) ->
    NodeTuple = list_to_tuple(NodeList),
	Sz = tuple_size(NodeTuple),
    N = find_first_gteq(NodeTuple, 1, Sz, FirstActionKey),
    {NodeKey, PointerInfo} = element(N, NodeTuple),
    {ok, ChildKPs, NewQueryOutput, State} = modify_node(Bt, PointerInfo, Action, QueryOutput),
	
	%% case ChildKPs = [] ???
	
    NewNodeList = lists:sort(lists:append(lists:delete({NodeKey, PointerInfo}, NodeList), ChildKPs)),	
	{ok, NewNodeList, NewQueryOutput, State}.
		
modify_kvnode(Bt, NodeList, {ActionType, ActionKey, ActionValue}, QueryOutput) ->
	case ActionType of
	insert ->
		NewNodeList = lists:sort(lists:append(NodeList, [{ActionKey, ActionValue}])),
		{ok, NewNodeList, QueryOutput, Bt}; 
	remove ->
        NodeTuple = list_to_tuple(NodeList),
        N = find_first_gteq(NodeTuple, 1, size(NodeTuple), ActionKey),
		NewNodeList = tuple_to_list(erlang:delete_element(N, NodeTuple)),
		{ok, NewNodeList, QueryOutput, Bt}; 
	fetch -> %% the QueryOutput is format {{Key, Value}, RightNode} and maybe there is another case for Can not find the N {Key, Value}
        NodeTuple = list_to_tuple(NodeList),
        N = find_first_gteq(NodeTuple, 1, size(NodeTuple), ActionKey),
		NewNodeList = tuple_to_list(NodeTuple),
	    {ok, {}, {N, NewNodeList, QueryOutput}, Bt} 
	end.
	
write_node(Bt, RootPointer, RightPointer, NodeType, NodeList) ->
    % split up nodes into smaller sizes
    NodeListList = chunkify(NodeList),
	case size(NodeListList) > 1 of
	true ->
		 {FirstList, SecondList} = NodeListList;
	false ->
         FirstList = NodeList, 
         SecondList = nil
    end,

    SecondKV = 
    case SecondList =:= nil of
    false ->
		 [{SecondKey, _} | _] = SecondList,
         SecondNode = #node{type = NodeType, subnum =  length(SecondList), subnodes = SecondList, rb_node = RightPointer},
         {State, SecondPointer} = save_node(Bt, SecondNode),
         {SecondKey, SecondPointer};
    true ->
		 State = Bt,
         SecondPointer = nil,
         nil
    end,
		
	[{FirstKey, _} | _] = FirstList,
    FirstKV = {FirstKey, RootPointer},
    FirstRightPointer =  
     case SecondPointer =:= nil of 
     false ->
          SecondPointer;
     true -> 
          RightPointer
     end, 
    
    FirstNode = #node{type = NodeType, subnum =  length(FirstList), subnodes = FirstList, rb_node = FirstRightPointer},
    save_node(FirstNode, RootPointer),	

	case SecondKV =:= nil of 
	true ->
	   {State, [FirstKV]};
	false ->
	   {State, [FirstKV, SecondKV]}
	end.	

%%========================================================================================================
%for the K gt than the rightest leaf then can call append_node directly.
%PointList = [{K0, Point0}, {K1, point1}....{Kn-1, Pointn-1}] Level 0 to Level n-1.
%Action = ActionType, ActionKey, ActionValue.
%QueryOutput : the law level rightest node pointer
%%========================================================================================================
append_node(Bt, [], Actions, QueryOutput) ->
	case QueryOutput of
	[] ->
		NodeType = kv_node;
    _Else ->
		NodeType = kp_node
    end,
	[{FirstKey, _} | _] = Actions,
	Node = #node{type = NodeType, rb_node = undefined,subnum = length(Actions), subnodes = Actions},
	{State, Pointer} = save_node(Bt, Node),
    {State, lists:append([{FirstKey, Pointer}], QueryOutput)};

append_node(Bt, KVList, Actions, QueryOutput) ->
    {Key, Pointer} = lists:last(KVList),
	Node = get_node(Pointer),
	#node{type = Type, rb_node = Rb_Node, subnum = SubNum, subnodes = SubNodes} = Node,
	MergedNodes = lists:sort(lists:append(SubNodes, Actions)),
	case SubNum < ?MAX_SUBNODE of
    true ->
	  NewNode = #node{type = Type, rb_node = Rb_Node, subnum = length(MergedNodes), subnodes = MergedNodes},
	  save_node(NewNode, Pointer),
	  {Bt, lists:append(KVList, QueryOutput)};
    false ->
	  {OldSubNodes, NewSubNodes} = lists:split(?MAX_SUBNODE, MergedNodes),
	  NewNode = #node{type = Type, rb_node = undefined, subnum = length(NewSubNodes), subnodes = NewSubNodes},
      {State, NewPointer} = save_node(Bt, NewNode),
	  [{NewKey, _} | _] = NewSubNodes,	 
	  [{OldKey, _} | _] = OldSubNodes,
	  OldNode = #node{type = Type, rb_node = NewPointer, subnum = length(OldSubNodes), subnodes = OldSubNodes},
	  save_node(OldNode, Pointer),
	 
	  NewKVList = lists:delete({Key, Pointer}, KVList),
	  NewQueryOutput = lists:append([{NewKey, NewPointer}], QueryOutput),
	  case NewKVList of 
	  [] ->
	      append_node(State, [], [{OldKey, Pointer}, {NewKey, NewPointer}], NewQueryOutput);
      _NewKVList ->	      
          append_node(State, _NewKVList, [{NewKey, NewPointer}], NewQueryOutput)
      end
	end.
	
addBlock(Index, {ActionKey, ActionValue}, State) ->
   Bt = State,
   PointerList = 
   case ets:lookup(?MEMTAGHEADERS, Index) of
   [] -> [];
   [#header{pointers = Pointers} = Header] -> Pointers
   end,
   
   case PointerList of
	   [] ->
		   {NewSate, ResultList} = append_node(Bt, [], [{ActionKey, ActionValue}], []),
		   save_Header(#header{index = Index, pointers = ResultList}),
		   {ok, NewSate};
	   _PointerList ->    
		   {LastKey, _} = lists:last(_PointerList),
		   case LastKey =< ActionKey of
		   true ->
			  {NewState, RightPointerList} = append_node(Bt, _PointerList, [{ActionKey, ActionValue}], []),
			  case RightPointerList of
			   _PointerList ->
				  NewHeader = #header{index = Index, pointers = _PointerList};
			  _RightPointerList -> 
                  NewHeader = #header{index = Index, pointers = _RightPointerList},
				  save_Header(NewHeader)
              end,
			  {ok, NewState};
		   false ->
			 [{_, FirstPointer} | _ ] = _PointerList, 
			 {ok, ResultList, QueryOutput, NewBt} = modify_node(Bt, FirstPointer, {insert, ActionKey, ActionValue}, _PointerList),
			 case length(ResultList) > 1 of 
			 true ->
				 Node = #node{type = kp_node, subnum = 2, subnodes = ResultList},
				 [{RootKey, _} | _] = ResultList,
                 {NewState, RootPointer} = save_node(NewBt, Node),
				 
				 % rightest list maybe changed maybe not
				 NewHeader = #header{index = Index, pointers = lists:append([{RootKey, RootPointer}], QueryOutput)},
				 save_Header(NewHeader),
				 {ok, NewState};
			 false ->
				 {ok, NewBt}
			 end
		   end
   end.

addBlockCall(Index, {ActionKey, ActionValue}, State) ->
   Bt = State,
   PointerList = 
   case ets:lookup(?MEMTAGHEADERS, Index) of
   [] -> [];
   [#header{pointers = Pointers} = Header] -> Pointers
   end,
   
   case PointerList of
	   [] ->
		   {NewSate, ResultList} = append_node(Bt, [], [{ActionKey, ActionValue}], []),
		   save_Header(#header{index = Index, pointers = ResultList}),
		   {ok, NewSate};
	   _PointerList ->    
		   {LastKey, _} = lists:last(_PointerList),
		   case LastKey =< ActionKey of
		   true ->
			  {NewState, RightPointerList} = append_node(Bt, _PointerList, [{ActionKey, ActionValue}], []),
			  case RightPointerList of
			   _PointerList ->
				  NewHeader = #header{index = Index, pointers = _PointerList};
			  _RightPointerList -> 
                  NewHeader = #header{index = Index, pointers = _RightPointerList},
				  save_Header(NewHeader)
              end,
			  {ok, NewState};
		   false ->
			 [{_, FirstPointer} | _ ] = _PointerList, 
			 {ok, ResultList, QueryOutput, NewBt} = modify_node(Bt, FirstPointer, {insert, ActionKey, ActionValue}, _PointerList),
			 case length(ResultList) > 1 of 
			 true ->
				 Node = #node{type = kp_node, subnum = 2, subnodes = ResultList},
				 [{RootKey, _} | _] = ResultList,
                 {NewState, RootPointer} = save_node(NewBt, Node),
				 
				 % rightest list maybe changed maybe not
				 NewHeader = #header{index = Index, pointers = lists:append([{RootKey, RootPointer}], QueryOutput)},
				 save_Header(NewHeader),
				 {ok, NewState};
			 false ->
				 {ok, NewBt}
			 end
		   end
   end.

searchItem(Index, Key, State) ->
   PointerList = 
   case ets:lookup(?MEMTAGHEADERS, Index) of
   [] -> [];
   [#header{pointers = Pointers} = Header] -> Pointers
   end,
   
   case PointerList of
	   [] ->
		   {ok, {}, State};
	   _PointerList ->    
		   [{_, FirstPoint} | _] = _PointerList,
           search_node(FirstPoint, Key, State)
   end.  

search_kvnode(NodeList, Key, QueryOutput) ->
    NodeTuple = list_to_tuple(NodeList),
    N = find_first_gteq(NodeTuple, 1, size(NodeTuple), Key),
	{ok, element(N, NodeTuple)}.


search_node(RootPointer, Key, QueryOutput)->
	case RootPointer of
	nil -> {ok, {}};
	_Pointer ->
	    #node{type = NodeType, subnodes = NodeList} = get_node( _Pointer),
		{ok, Kv} = 
		case NodeType of 
		kp_node -> search_kpnode(NodeList, Key, QueryOutput);    		 
		kv_node -> search_kvnode(NodeList, Key, QueryOutput)
		end,
	    {ok, Kv}
	end.

search_kpnode(NodeList, Key,  QueryOutput) ->
    NodeTuple = list_to_tuple(NodeList),
	Sz = tuple_size(NodeTuple),
    N = find_first_gteq(NodeTuple, 1, Sz, Key),
    {_, PointerInfo} = element(N, NodeTuple),
    {ok, NewQueryOutput} = search_node(PointerInfo, Key, QueryOutput),
	{ok, NewQueryOutput}.



%================================================================================================================
%  I/O the node from file
%================================================================================================================

get_node(PosPoint) ->
   File_Id = PosPoint#pointer.file_id,
   Pos = PosPoint#pointer.pos,
   {ok, Fd}  = openFile(File_Id),
   Size = ?NODESIZE,
   file:position(Fd, Pos),
   case file:read(Fd, Size) of
      {ok, Data} ->
           Node = binary_to_term(zlib:unzip(Data));
      {error, Reason} ->
           Node = undefined;
      eof -> 
           Node = undefined
    end,
    Node.		
		
save_node(Node, Pointer) when is_record(Node, node)->
     File_Id  = Pointer#pointer.file_id,
	 File_Pos = Pointer#pointer.pos,
	 {ok, Fd} = openFile(File_Id),
	 Data = zlib:zip(term_to_binary(Node)),
     file:position(Fd, File_Pos),
	 
     case file:write(Fd, Data) of
         ok -> ok;
         {error, Reason} -> {error, Reason}
	end;
		
save_node(Bt, Node) when is_record(Bt, state) -> 
    {ok, Pointer, NewFreeSpaces} = get_Pointer(Bt#state.freespaces, Bt#state.basedir),
    save_node(Node, Pointer),
	{Bt#state{freespaces = NewFreeSpaces}, Pointer}.

get_Pointer(FreeSpaces, BaseDir) ->
	FindSize = ?NODESIZE, 
    case findnearSize(FindSize, FreeSpaces) of
	  {ok, FSpace} -> 
            DSpaces = lists:delete(FSpace, FreeSpaces),
	        dets:delete_object(indexfreespace, FSpace),
			ResultPointer = #pointer{file_id = FSpace#freespace.file_id, pos = FSpace#freespace.head_pos - FindSize},
			if (FSpace#freespace.free_size - FindSize) >= FindSize ->
			   ISpace = #freespace{free_size = FSpace#freespace.free_size - FindSize, file_id = FSpace#freespace.file_id, head_pos = FSpace#freespace.head_pos - FindSize},	
			   dets:insert(indexfreespace, ISpace),
			   RSpaces = lists:sort(fun(X,Y) -> {X#freespace.free_size, X#freespace.file_id} < {Y#freespace.free_size, Y#freespace.file_id} end, lists:append([ISpace], DSpaces)),
			   {ok, ResultPointer, RSpaces};
			true ->   
			   {ok, ResultPointer, DSpaces}
			end;
      {false} ->
			FIdx = intelDB_TagBase:getIndexAbsSeq(),
			FId  = intelDB_TagBase:getIndexRelSeq(),
			{ok, FreeSize, Pos} = createFile(FIdx, FId, BaseDir),
			NSpace = #freespace{free_size = FreeSize - FindSize, file_id = FId, head_pos = Pos - FindSize},
			dets:insert(indexfreespace, NSpace),
			RSpaces = lists:sort(fun(X,Y) -> {X#freespace.free_size, X#freespace.file_id} < {Y#freespace.free_size, Y#freespace.file_id} end, lists:append([NSpace], FreeSpaces)),
		    {ok, #pointer{file_id = FId, pos = Pos - FindSize}, RSpaces}	   
    end.

chunkify(InList) ->
    case length(InList) of
    Size when Size > ?MAX_SUBNODE ->
		Mid = trunc(Size/2 + 0.5),
		NodeListList = lists:split(Mid, InList);
    _Else ->
        {InList}
    end.

find_first_gteq(_Tuple, Start, End, _Key) when Start == End ->
    End;
find_first_gteq(Tuple, Start, End, Key) ->
    Mid = Start + ((End - Start) div 2),
    {TupleKey, _} = element(Mid, Tuple),
    case TupleKey =< Key of
    true ->
		{NextKey, _} = element(Mid+1, Tuple),
		case Key < NextKey of
		true -> Mid;
		false -> 
           find_first_gteq(Tuple, Mid+1, End, Key)
		end;    
	false ->
        find_first_gteq(Tuple, Start, Mid, Key)
    end.

ensureBase() ->
	BaseDir = getBaseDir(),
	case intelDB_util:ensureDir(BaseDir, ?DEF_INDXDIR) of
	 {ok, NewDir} -> NewDir;
	 {error, Reason} ->
	    %% logger the error 'can not create folder for ' + NewDir.
	    stop() 	 
	end.	 
		
getBaseDir() ->
	case application:get_env(intelDB, indexBaseDir) of
		{ok, IndxBaseDir} ->
		 IndxBaseDir;
		undefined ->
		 ?DEF_INDXDIR
	end.
	
%================================================================================================================
%for init when the ?HEADERS file does not exist then create defult file
%================================================================================================================
saveHeader(N) ->
	if N < ?DEF_TAGCOUNT ->
	   Tagheader = #header{index = N,  pointers = []},
       ets:insert_new(?MEMTAGHEADERS, Tagheader),
	   saveHeader(N + 1);
	true -> ok
	end.





%=======================================================================
test(StN, EdN) ->
	start(),
	testAdd(StN, EdN).

testAdd(N, M) ->
	case N < M of
	true ->
	  addArchBlock(120, {N, 100*N}),
	  testAdd(N+1, M);
	false ->
	  {ok}
    end.	  

testCnt(M) ->
    testAdd(M, 8000 + M).

testGet() ->
	start(),
	getAllItems(120).
	



testSearch(Index, Key) ->
	{Key, Value} = searchItem(Index, Key),
    io:fwrite("------------------------Key : ~p--------Value : ~p   -------------------------------------------~n", [Key, Value]).



%=======================================================================

	
getBlock(Index, State) ->
   PointerList = 
   case ets:lookup(?MEMTAGHEADERS, Index) of
   [] -> [];
   [#header{pointers = Pointers} = Header] -> Pointers
   end,
   
   case PointerList of
   [] ->
	   {ok, State};
	_PointerList ->
	   [{_, RootPointer} | _] = _PointerList,
	   printBTree(1, RootPointer, State)
   end.	


printBTree(N, RootPointer, State) ->
     io:fwrite("============================================Level : ~p Start=======================================================================~n", [N]),
	 Node = get_node(RootPointer),
     printSubTree(1, Node, State, 0),
     io:fwrite("=============================================Level : ~p End========================================================================~n~n", [N]),       
     case Node#node.type of 
     kv_node ->
		  io:fwrite("==================~n"),
		{ok, State}; 
     kp_node ->
	    SubNodeList = Node#node.subnodes,
        [{_, FirstPointer} | _] = SubNodeList,
        printBTree(N + 1, FirstPointer, State)
     end.

printSubNodes(N, []) ->
    io:fwrite("---------------------------------------------------------------------------------------------------------------~n");
	


printSubNodes(N, [{Key, Value} | Rest]) ->
    io:fwrite("----------------Sub Node : ~p---------Key : ~p--------Value : ~p   -------------------------------------------~n", [N, Key, Value]),
    printSubNodes(N + 1, Rest).
	


printSubTree(N, Node, State, M) ->
   io:fwrite("----------------Node:~p-------------------- ~p ----------------------------------------~n", [N, Node#node.subnum]),
   printSubNodes(1, Node#node.subnodes),
   case Node#node.rb_node of
	   undefined ->
           io:fwrite("------------------------------------~p------------------------------------------------~n", [M + Node#node.subnum]);		   
       _RightPointer ->
	       RightNode = get_node( _RightPointer),
           printSubTree(N + 1, RightNode, State, M + Node#node.subnum)
   end.
	 

