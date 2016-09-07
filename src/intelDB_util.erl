%% @author jliu4
%% @doc @todo Add description to intelDB_util.


-module(intelDB_util).

%% ====================================================================
%% API functions
%% ====================================================================
-export([ensureDir/2]).
-export([getVersion/0]).
-export([getMaxFileSize/0]).
-export([stamptomsecs/1]).

-define(VERSION, 1000000).
-define(MAX_FILESIZE, 512*1024*1024).


%% ====================================================================
%% Internal functions
%% ====================================================================
getVersion() ->
	?VERSION.

getMaxFileSize() ->
	?MAX_FILESIZE.

ensureDir(Dir, Default) ->
	NewDir =case Dir of
	   "" -> Default;
	   _ -> Dir
	end,
	case filelib:ensure_dir(NewDir) of
         ok -> {ok, NewDir};
         {error, Reason} -> {error, Reason}
    end.


	
stamptomsecs({MegaSecs,Secs,MicroSecs} = Timestamp) ->
  (MegaSecs*1000000 + Secs)*1000000 + MicroSecs.	
	

