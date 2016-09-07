%% @author Administrator
%% @doc @todo Add description to intelDB_TagBase.


-module(intelDB_TagBase).

%%
-author('Jie-Liu').

-behaviour(gen_server).
-compile(export_all).

-include_lib("stdlib/include/qlc.hrl").

-define(VERSION, 1000000).
-define(TEMP_FILE, 200).

-define(ARCH_RELSQU, 0).
-define(ARCH_ABSSQU, 1).
-define(INDX_RELSQU, 10).
-define(INDX_ABSSQU, 11).
-define(TEMP_RELSQU, 20).
-define(TEMP_ABSSQU, 21).
-define(TAGS_INDEX,30).

-define(SEQUENCE, sequence).
-define(TAGSBASE, memTagsbase). %% index-parame1, pareme2....
-define(INDEXBASE, memIndexbase). %% free indexs base: {index, nextIndex}
-define(TAGNAME, memTagname).     %% {tagName, index}
-define(TAGS_PREX, "TagsBase").
-define(TAGS_INEX, "TagIndex").
-define(TAGS_NAME, "TagName").
-define(SEQ_PREX, "Sequence").

-define(DEF_TAGSDIR, "c:/IntelDB/data/tagsBase/").
-define(DEF_TAGCOUNT, 1000000).
-define(DEF_MAXID, 2000000).

-record(sequence, {
				 id,   %% the Id of squence 0 : Index File;
                 value = 0 :: integer %% the squence value
				 } ).


-record(tagsInfo, {
				  tagName :: list,
                  precision :: integer,
				  absolute :: float,
				  relative :: float				  
				  }).

-record(tagsBase, {
				  index :: integer, %% the index of Tag
                  precision :: integer,
				  absolute :: float,
				  relative :: float				  
				  }).

-record(indexBase, {
				  index :: integer, %% the index of Tag
                  value :: integer  %% the id of tag
				  }).

-record(nameIndex, {
					tagName :: list,
					index  :: integer
					}).

-record(state, {
                basedir = [] :: list, %% the base dir of tempDir 
                errors  = [] :: list               
               }).

getTagCount() ->
	gen_server:call(?MODULE, {getTagCount},  infinity).

getTagInforByName(TagName) ->
	gen_server:call(?MODULE, {getTagInfoByName, TagName},  infinity).


getTagInforByIndex(Index) ->
	gen_server:call(?MODULE, {getTagInfoByIndex, Index},  infinity).
	
getTempRelSeq() ->
	gen_server:call(?MODULE, {getSequence, ?TEMP_RELSQU},  infinity).

getTempAbsSeq() ->
	gen_server:call(?MODULE, {getSequence, ?TEMP_ABSSQU},  infinity).

getArchRelSeq() ->
	gen_server:call(?MODULE, {getSequence, ?ARCH_RELSQU},  infinity).

getArchAbsSeq() ->
	gen_server:call(?MODULE, {getSequence, ?ARCH_ABSSQU},  infinity).

getIndexRelSeq() ->
	gen_server:call(?MODULE, {getSequence, ?INDX_RELSQU},  infinity).

getIndexAbsSeq() ->
	gen_server:call(?MODULE, {getSequence, ?INDX_ABSSQU},  infinity).

%%=======================================================================================

addTagInfo(TagInfo) ->
	gen_server:call(?MODULE, {addTagInfo, TagInfo},  infinity).
	
deleteTagInfo(TagInfo) ->
	gen_server:call(?MODULE, {deleteTagRecord, TagInfo},  infinity).

updateTagInfo(TagName, TagInfo) ->
	gen_server:call(?MODULE, {updateTagRecord, TagName,TagInfo},  infinity).
	
%% ====================================================================
%% Internal functions
%% ====================================================================

init([Name, Options]) ->
	BaseDir = ensureBase(),
	initTables(),
	load_SequenceFromDB(BaseDir),
	initIndexBase(BaseDir),
	load_TagsBaseFromDB(BaseDir),
	load_NameIndex(BaseDir),
    {ok, #state{basedir = BaseDir}}.

initTables() ->
	ets:new(?SEQUENCE, [private,ordered_set, named_table, {keypos, #sequence.id}, {read_concurrency, true}, {write_concurrency, true}]), 
	%% #tagsBase
	ets:new(?TAGSBASE, [private, ordered_set, named_table, {keypos, 1}, {read_concurrency, true}, {write_concurrency, true}]),
	%% #nameIndex
	ets:new(?TAGNAME, [private, ordered_set, named_table, {keypos, 1}, {read_concurrency, true}, {write_concurrency, true}]). 

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

handle_cast(stop, State) ->
   closeTables(),
   {stop, normal, State}.

handle_call({getTagCount}, _From, _State) ->
    TagCount =
	case ets:info(?INDEXBASE, size) of
     undefined ->
		 ?DEF_TAGCOUNT;
	 0 ->	
		 ?DEF_TAGCOUNT;
	 Value -> 
		 Value
    end,
	{reply, TagCount, _State};

handle_call({addTagInfo, TagInfo}, _From, _State) ->
	{reply, addTagRecord(TagInfo), _State};

handle_call({deleteTagInfo, TagInfo}, _From, _State) ->
	{reply, deleteTagInfo(TagInfo), _State};

handle_call({updateTagInfo, TagName, TagInfo}, _From, _State) ->
	{reply, updateTagRecord(TagName, TagInfo), _State};

handle_call({getSequence, Index}, _From, _State) ->
	{reply, getSquenct(Index), _State};

handle_call({getTagInfoByName, TagName}, _From, _State) ->
	ResTagInfo = getTagInfoByName(TagName), 
	{reply, ResTagInfo, _State};


handle_call({getTagInfoByIndex, Index}, _From, _State) ->
	ResTagInfo = getTagInfoByIndex(Index), 
	{reply, ResTagInfo, _State}.

load_SequenceFromDB(BaseDir) ->
	FileSeq =  BaseDir ++ ?SEQ_PREX ++ ".dat",
	dets:open_file(sequence, [{file, FileSeq}, {type, set}, {auto_save, 10}, {keypos, #sequence.id}]),
    ets:from_dets(?SEQUENCE, sequence).

load_TagsBaseFromDB(BaseDir) ->
	FileName =  BaseDir ++ ?TAGS_PREX ++ ".dat",
	dets:open_file(tagsBase, [{file, FileName}, {type, set}, {auto_save, 10}, {keypos, 1}]),
    ets:from_dets(?TAGSBASE, tagsBase).

load_NameIndex(BaseDir) ->
	FileName =  BaseDir ++ ?TAGS_NAME ++ ".dat",
	dets:open_file(tagsName, [{file, FileName}, {type, set}, {auto_save, 10}, {keypos, 1}]),
    ets:from_dets(?TAGNAME, tagsName).

closeTables() ->
   dets:close(sequence),
   dets:close(tagIndex),	
   dets:close(tagsBase).	

initIndexBase(BaseDir) ->
  FileName = BaseDir ++ ?TAGS_INEX ++ ".dat",
  dets:open_file(tagIndex, [{file, FileName}, {type, set}, {auto_save, 10}, {keypos, 1}]).

getTagInfoByName(TagName) ->	
	case ets:lookup(?TAGNAME, TagName) of
		[] -> [];
		[{TagName, Index}] ->
			case ets:lookup(?TAGSBASE, Index) of
	         [{Index, Precision, Absolute, Relative}] ->
	                #tagsInfo{tagName = TagName, precision = Precision, absolute = Absolute, relative = Relative};
	         [] -> []
	        end
	end.

getTagInfoByIndex(Index) ->	
	case ets:lookup(?TAGSBASE, Index) of
	[] ->
		#tagsBase{index = Index, precision = 3, absolute = 0.01, relative = 1.00};
	[Res] ->
		Res
	end.
 
getTagIndex() ->
   case dets:info(tagIndex, size) of
      0 -> getSquenct(?TAGS_INDEX);
	  _ -> Index = dets:first(tagIndex),
		   dets:delete(tagIndex, Index),
		   Index
   end.

deleteTagRecord(TagInfo) ->
  TagName = TagInfo#tagsInfo.tagName,
  case ets:lookup(?TAGNAME, TagName) of
	[{_TagName, _Index}] ->
	  ets:delete(?TAGSBASE, _Index),
	  dets:delete(tagsBase, _Index),
	  ets:delete(?TAGNAME, TagName),
	  dets:delete(tagsName, TagName),
      retIndexToBase(_Index);
	_ -> ok
  end.

updateTagRecord(TagName, TagInfo) ->
  case ets:lookup(?TAGNAME, TagName) of
	[{_TagName, _Index}] ->
	  ets:delete(?TAGNAME, TagName),
	  dets:delete(tagsName, TagName),
	  ets:insert(?TAGNAME, [{_Index, TagInfo#tagsInfo.tagName}]),
	  dets:insert(tagsName, [{_Index, TagInfo#tagsInfo.tagName}]),
	  ets:insert(?TAGSBASE, [{_Index, TagInfo#tagsInfo.precision, TagInfo#tagsInfo.absolute, TagInfo#tagsInfo.relative}]),
	  dets:insert(tagsBase, [{_Index, TagInfo#tagsInfo.precision, TagInfo#tagsInfo.absolute, TagInfo#tagsInfo.relative}]);
	_ -> ok
  end.

retIndexToBase(Index) ->
	dets:insert(tagIndex, [{Index}]).
  
addTagRecord(TagInfo) ->
  Index = getTagIndex(),
  TagName = TagInfo#tagsInfo.tagName,
  ets:insert(?TAGNAME, [{TagName, Index}]),
  dets:insert(tagsName, [{TagName, Index}]),
  ets:insert(?TAGSBASE, [{Index, TagInfo#tagsInfo.precision, TagInfo#tagsInfo.absolute, TagInfo#tagsInfo.relative}]),
  dets:insert(tagsBase, [{Index, TagInfo#tagsInfo.precision, TagInfo#tagsInfo.absolute, TagInfo#tagsInfo.relative}]),
  ok.
 	
getSquenct(Index) ->
	NextSeq =
	case ets:lookup(?SEQUENCE, Index) of
	[] ->
		#sequence{id = Index, value = 0};
	[Res] ->
		#sequence{id = Index, value = Res#sequence.value + 1}
	end,
    ets:insert(?SEQUENCE, NextSeq),
	dets:insert(sequence, NextSeq),
	NextSeq#sequence.value.

ensureBase() ->
	BaseDir = getBaseDir(),
	case intelDB_util:ensureDir(BaseDir, ?DEF_TAGSDIR) of
	 {ok, NewDir} -> NewDir;
	 {error, Reason} ->
	    %% logger the error 'can not create folder for ' + NewDir.
	    stop() 	 
	end.	 
		
getBaseDir() ->
	case application:get_env(intelDB, tagsBaseDir) of
		{ok, TagBaseDir} ->
		 TagBaseDir;
		undefined ->
		 ?DEF_TAGSDIR
	end.









%%===========================================================================
%%  test the function addArchBlock
%%
%%===========================================================================
addTagInfo(TagName, Precision, Absoluts, Relative) ->
	TagInfo = #tagsInfo{tagName = TagName, precision = Precision, absolute = Absoluts, relative = Relative},
	addTagInfo(TagInfo).

addTagTest(0) ->
	ok;
addTagTest(N) ->
	TagName = integer_to_list(N),
	addTagInfo(TagName, 3, 12.23, 24.46),
	addTagTest(N - 1).


addTest() ->
  addTagTest(600000).


getTagTest(0) ->
	ok;
getTagTest(N) ->
	TagName = integer_to_list(N),
	TagInfo = getTagInforByName(TagName),
	io:fwrite("Throw reason: ~p   ~p ~n", [TagInfo#tagsInfo.tagName, TagInfo#tagsInfo.precision]), 
	getTagTest(N - 1).

getTest() ->
  getTagTest(599990).


