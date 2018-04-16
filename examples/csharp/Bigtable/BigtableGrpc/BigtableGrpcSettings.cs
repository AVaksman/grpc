namespace BigtableGrpc
{
    public class BigtableGrpcSettings
    {
        public string InstanceId { get; set; }
        public string ProjectId { get; set; }
        public string RowKeyPrefix { get; set; }
        public int RowKeySize { get; set; }
        public long Records { get; set; }
        public int ScanTestDurationMinutes { get; set; }
        public long RowsLimit { get; set; }
        public int Channels { get; set; }
        public string TableName { get; set; }
    }
}
