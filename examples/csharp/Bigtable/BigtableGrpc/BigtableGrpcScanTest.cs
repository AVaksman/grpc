using System;
using System.Collections.Generic;
using System.Diagnostics;
using Google.Cloud.Bigtable.V2;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using Google.Protobuf;
using Grpc.Auth;
using Grpc.Core;
using HdrHistogram;

namespace BigtableGrpc
{
    class BigtableGrpcScanTest
    {
        private readonly BigtableGrpcSettings _settings;
        private int ReadErrors;
        private readonly BigtableTestClient myClient;
        private static string _stringFormat;
        private static string _table;

        internal BigtableGrpcScanTest(BigtableGrpcSettings settings)
        {
            _settings = settings;

            var path = Directory.GetCurrentDirectory();
            //ChannelCredentials channelCredentials = GoogleCredential.FromFile(path + "/Grass-Clump-479-b5c624400920.json").ToChannelCredentials();
            ChannelCredentials channelCredentials = GoogleCredential.FromComputeCredential().ToChannelCredentials();

            myClient = new BigtableTestClient(new Bigtable.BigtableClient(GetChannel(channelCredentials)));
        }

        internal async Task<int> Scan(LongConcurrentHistogram histogramScan)
        {
            _stringFormat = "D" + _settings.RowKeySize;
            _table = "projects/grass-clump-479/instances/" + _settings.InstanceId + "/tables/" + _settings.TableName;

            var runtime = Stopwatch.StartNew();

            ReadErrors = 0;

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Starting Scan test, instance {_settings.InstanceId} against table {_settings.TableName} for {_settings.ScanTestDurationMinutes} minutes at {_settings.RowsLimit} rows chunks");

            var rowsRead = 0;
            var r = new Random();

            while (runtime.Elapsed.TotalMinutes < _settings.ScanTestDurationMinutes)
            {
                var rowNum = r.Next(0, (int) _settings.Records);
                ByteString rowKey = ByteString.CopyFromUtf8( _settings.RowKeyPrefix + rowNum.ToString(_stringFormat));

                var readRowRerquest = ReadRowsRequestBuilder(rowKey);

                var startTime = runtime.Elapsed;
                try
                {
#if DEBUG
                    Console.WriteLine($"Scanning beginning with rowkey {rowKey.ToStringUtf8()}");
#endif
                    var response = myClient.SendReadRowsRequest(readRowRerquest);
                    rowsRead += await CheckReadAsync(response).ConfigureAwait(false);

#if DEBUG
                    var status = response.ResponseHeadersAsync.Status;
                    Console.WriteLine($"Status = {status}");
#endif
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Failed to read beginning rowkey {rowKey.ToStringUtf8()}, error message: {ex.Message}");
                    ReadErrors++;
                }
                finally
                {
                    var endReadTime = runtime.Elapsed;
                    var responseTime = endReadTime - startTime;
                    histogramScan.RecordValue((long)(responseTime.TotalMilliseconds * 100));
                }
            }

            return rowsRead;
        }

        public ReadRowsRequest ReadRowsRequestBuilder(ByteString rowKey) =>
            new ReadRowsRequest
            {
                TableName = _table,
                RowsLimit = _settings.RowsLimit,
                Filter = new RowFilter { CellsPerColumnLimitFilter = 1 }
            };

        internal async Task<int> CheckReadAsync(AsyncServerStreamingCall<ReadRowsResponse> responseRead)
        {
            var responseStream = responseRead.ResponseStream;
            int rowCount = 0;

            while (await responseStream.MoveNext())
            {
                var chunks = responseStream.Current.Chunks;
                foreach (var chunk in chunks)
                {
                    rowCount += chunk.RowKey.IsEmpty ? 0 : 1;
                }
            }

#if DEBUG
            Console.WriteLine($"Read {rowCount} rows");
#endif
            return rowCount;
        }

        private Channel GetChannel(ChannelCredentials channelCredentials)
        {
            // Set channel send/recv message size to unlimited.
            var channelOptions = new[]
            {
                new ChannelOption(ChannelOptions.MaxSendMessageLength, -1),
                new ChannelOption(ChannelOptions.MaxReceiveMessageLength, -1)
            };
            
            return new Channel("bigtable.googleapis.com", 443, channelCredentials, channelOptions);
        }

        public void WriteCsvToConsole(TimeSpan scanDuration, int rowsRead, LongConcurrentHistogram hScan)
        {
            try
            {
                var throughput = rowsRead / scanDuration.TotalSeconds;
                Console.WriteLine($"Scan operations={hScan.TotalCount:N0}, failed={ReadErrors:N0}, throughput={throughput:N}, 50th={hScan.GetValueAtPercentile(50) / 100.0:N}, 75th={hScan.GetValueAtPercentile(75) / 100.0:N}, 95th={hScan.GetValueAtPercentile(95) / 100.0:N}, 99th={hScan.GetValueAtPercentile(99) / 100.0:N}, 99.9th={hScan.GetValueAtPercentile(99.9) / 100.0:N}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Exception writing test results to console error: {ex.Message}");
            }
        }

        public void WriteCsv(TimeSpan scanDuration, int rowsRead, LongConcurrentHistogram hScan)
        {
            string path = Directory.GetParent(Directory.GetCurrentDirectory()).ToString() + $"/csv/scan_test_{DateTime.Now:MM_dd_yy_HH_mm}.csv";

            try
            {
                var throughput = rowsRead / scanDuration.TotalSeconds;
                using (var writetext = new StreamWriter(path))
                {
                    writetext.WriteLine();
                    writetext.WriteLine(
                        "Stage, Operation Name, Chunk Size, Run Time, Max Latency, Min Latency, Operations, Throughput, p50 latency, p75 latency, p95 latency, p99 latency, p99.99 latency, Success Operations, Failed Operations");
                    writetext.WriteLine(
                        $"Scan , Read, {_settings.RowsLimit}, {scanDuration.TotalSeconds:F}, {hScan.Percentiles(5).Max(a => a.ValueIteratedTo) / 100.0}, {hScan.Percentiles(5).Min(a => a.ValueIteratedTo) / 100.0}, {hScan.TotalCount}, {throughput:F0}, {hScan.GetValueAtPercentile(50) / 100.0}, {hScan.GetValueAtPercentile(75) / 100.0}, {hScan.GetValueAtPercentile(95) / 100.0}, {hScan.GetValueAtPercentile(99) / 100.0}, {hScan.GetValueAtPercentile(99.99) / 100.0}, {hScan.TotalCount - ReadErrors}, {ReadErrors}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Exception writing test results into {path}, error: {ex.Message}");

            }
        }
    }
}
