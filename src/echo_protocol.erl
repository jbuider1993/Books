-module(echo_protocol).
-behaviour(ranch_protocol).

-export([start_link/4]).
-export([init/4]).

-record(cst,
		    {buf = <<>> :: binary() 
			}).

start_link(Ref, Socket, Transport, Opts) ->
	Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
	{ok, Pid}.

init(Ref, Socket, Transport, _Opts = []) ->
	ok = ranch:accept_ack(Ref),
	loop(Socket, Transport, #cst{}).

processPackage(Data) when byte_size(Data) =:= 0 -> ok;

processPackage(Data) ->
	DgramSize = byte_size(Data),
		try
			<<FCommand:16/native-signed-integer-unit:1, FTagId:64/unsigned-integer-unit:1 ,FDate:64/float-unit:1,FValue:64/float-unit:1, FState:8/native-signed-integer-unit:1, LData/binary>> = Data,
			intelDB_BuffManager:addData(FTagId, FDate, FValue, FState),
			processPackage(LData)
		catch
			error:Reason ->
				io:fwrite("Error reason: ~p~n", [Reason]);
			throw:Reason ->
				io:fwrite("Throw reason: ~p~n", [Reason]);
			exit:Reason ->
				io:fwrite("Exit reason: ~p~n", [Reason])			
		end.

loop(Socket, Transport, Cst) ->
	case Transport:recv(Socket, 0, 30000) of
		{ok, Data} ->
			if is_binary(Data) ->
				    Buff0 = Cst#cst.buf,
					Cst1 = Cst#cst{buf = <<Buff0/binary, Data/binary>>},
					case Cst1#cst.buf of
				        <<PacketLength:32/integer-unit:1, ClientPayload/binary>> ->
							RecLength = size(ClientPayload),
							case size(ClientPayload) of
								 PacketLength ->									  			
					                  Transport:send(Socket, <<PacketLength:32/integer>>),
				                      processPackage(ClientPayload),
									  loop(Socket,Transport, #cst{buf = <<>>});
								 _ ->
									  loop(Socket,Transport, Cst1)
							end;	
		                _ ->
							loop(Socket,Transport, #cst{buf = <<>>})
					end;
			   true ->
			        loop(Socket, Transport, #cst{buf = <<>>})
			end,
			loop(Socket, Transport, #cst{buf = <<>>});
		_ ->
			ok = Transport:close(Socket)
	end.
