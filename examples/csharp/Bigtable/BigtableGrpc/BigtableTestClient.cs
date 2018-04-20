using Google.Cloud.Bigtable.V2;
using Grpc.Core;

namespace ReadWriteGrpc
{
    public class BigtableTestClient
    {
        private readonly Bigtable.BigtableClient client;

        public BigtableTestClient(Bigtable.BigtableClient client)
        {
            this.client = client;
        }

        internal AsyncServerStreamingCall<MutateRowsResponse> SendMutateRowsRequest(MutateRowsRequest mutateRowsRequest)
        {
            //var mutateRowsRequest = new MutateRowsRequest();
            //add entries over here
            return client.MutateRows(mutateRowsRequest);
        }

        internal MutateRowResponse SendMutateRowRequest(MutateRowRequest mutateRowRequest)
        {
            //var mutateRowsRequest = new MutateRowsRequest();
            //add entries over here
            return client.MutateRow(mutateRowRequest);
        }

        internal AsyncServerStreamingCall<ReadRowsResponse> SendReadRowsRequest(ReadRowsRequest readRowsRequest) => 
            client.ReadRows(readRowsRequest, new CallOptions());

    }
}
