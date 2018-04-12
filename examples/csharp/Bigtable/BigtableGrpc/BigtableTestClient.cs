using System.Collections.Generic;
using System.Threading.Tasks;
using Google.Cloud.Bigtable.V2;
using Grpc.Core;

namespace BigtableGrpc
{
    public class BigtableTestClient
    {
        private readonly Bigtable.BigtableClient client;

        public BigtableTestClient(Bigtable.BigtableClient client)
        {
            this.client = client;
        }

        internal void SendMutateRowRequest()
        {
            var mutateRowsRequest = new MutateRowsRequest();
            //add entries over here
            client.MutateRows(mutateRowsRequest);
        }

        internal AsyncServerStreamingCall<ReadRowsResponse> SendReadRowsRequest(ReadRowsRequest readRowsRequest) => 
            client.ReadRows(readRowsRequest);

    }
}
