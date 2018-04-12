@rem Copyright 2016 gRPC authors.
@rem
@rem Licensed under the Apache License, Version 2.0 (the "License");
@rem you may not use this file except in compliance with the License.
@rem You may obtain a copy of the License at
@rem
@rem     http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.

@rem Generate the C# code for .proto files

setlocal

@rem enter this director
cd /d %~dp0

set TOOLS_PATH=packages\Grpc.Tools.1.10.1-pre1\tools\windows_x86
%TOOLS_PATH%\protoc.exe -I../../protos --csharp_out BigtableGrpc  ../../protos/google/bigtable/v2/bigtable.proto --grpc_out BigtableGrpc --plugin=protoc-gen-grpc=%TOOLS_PATH%\grpc_csharp_plugin.exe
%TOOLS_PATH%\protoc.exe -I../../protos --csharp_out BigtableGrpc  ../../protos/google/bigtable/v2/data.proto --grpc_out BigtableGrpc --plugin=protoc-gen-grpc=%TOOLS_PATH%\grpc_csharp_plugin.exe
%TOOLS_PATH%\protoc.exe -I../../protos --csharp_out BigtableGrpc  ../../protos/google/rpc/code.proto --grpc_out BigtableGrpc --plugin=protoc-gen-grpc=%TOOLS_PATH%\grpc_csharp_plugin.exe
%TOOLS_PATH%\protoc.exe -I../../protos --csharp_out BigtableGrpc  ../../protos/google/rpc/status.proto --grpc_out BigtableGrpc --plugin=protoc-gen-grpc=%TOOLS_PATH%\grpc_csharp_plugin.exe
%TOOLS_PATH%\protoc.exe -I../../protos --csharp_out BigtableGrpc  ../../protos/google/api/annotations.proto --grpc_out BigtableGrpc --plugin=protoc-gen-grpc=%TOOLS_PATH%\grpc_csharp_plugin.exe
%TOOLS_PATH%\protoc.exe -I../../protos --csharp_out BigtableGrpc  ../../protos/google/api/http.proto --grpc_out BigtableGrpc --plugin=protoc-gen-grpc=%TOOLS_PATH%\grpc_csharp_plugin.exe


endlocal
