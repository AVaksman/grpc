using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Bigtable.Admin.V2;
using Google.Cloud.Bigtable.V2;
using Google.Protobuf;
using Grpc.Auth;
using Grpc.Core;
using HdrHistogram;

namespace ReadWriteGrpc
{
    class ReadWriteGrpcTest
    {
        public static ByteString[] Rand;
        private readonly ReadWriteGrpcSettings _settings;
        private readonly object lockObj = new object();
        private static readonly Random _rnd = new Random();
        private static readonly List<BigtableTestClient> BTtestClientPool = new List<BigtableTestClient>();
        private static string Type { get; set; }
        private static string _stringFormat = "D7";
        private static string _table;
        private int failCtr;
        private const int RandSize = 256;
        private static int ChPoolSize;
        private static int chNum;
#if DEBUG
        private int _recordsWritten;
#endif

        internal ReadWriteGrpcTest(ReadWriteGrpcSettings settings)
        {
            _settings = settings;
            var path = Directory.GetCurrentDirectory();
            ChannelCredentials channelCredentials = GoogleCredential.FromFile(path + "/Grass-Clump-479-b5c624400920.json").ToChannelCredentials();
            //ChannelCredentials channelCredentials = GoogleCredential.FromComputeCredential().ToChannelCredentials();

            ChPoolSize = _settings.Channels;
            FillChannelPool(channelCredentials);
        }

        internal void LoadTest(LongConcurrentHistogram loadHistogram)
        {
            _table = SetTableName();
            Type = "loadTest";

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Instance: {_settings.InstanceId}");
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Starting {Type} test against Table: {_settings.TableName} for {_settings.Records} records using {_settings.LoadThreads} threads");

            var startIdCount = _settings.Records / _settings.Batchsize;
            var startIdsList = new List<int>();

            for (int i = 0; i < startIdCount; i++)
            {
                startIdsList.Add(i * _settings.Batchsize);
            }
            startIdsList.Reverse();
            var startIds = new System.Collections.Concurrent.ConcurrentBag<int>(startIdsList);

            using (var IdIterator = startIds.GetEnumerator())
            {
                List<Thread> threads = new List<Thread>();

                var runtime = Stopwatch.StartNew();

                for (int i = 0; i < _settings.LoadThreads; i++)
                {
                    threads.Add(new Thread(() =>
                    {
                        while (IdIterator.MoveNext())
                        {
                            var startId = IdIterator.Current;

                            ByteString rowKey = ByteString.Empty;
                            MutateRowsRequest request = new MutateRowsRequest
                            {
                                TableName = _table
                            };

                            for (int j = 0; j < _settings.Batchsize && startId < _settings.Records; j++)
                            {
                                rowKey = ByteString.CopyFromUtf8(_settings.RowKeyPrefix + startId++.ToString(_stringFormat));
                                var entry = new MutateRowsRequest.Types.Entry
                                {
                                    RowKey = rowKey,
                                    Mutations = { MutationsBuilder()}
                                };

                                request.Entries.Add(entry);
                            }

                            var startTime = runtime.Elapsed;

                            try
                            {
#if DEBUG
                                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} inserting record {startId} last rowkey {rowKey.ToStringUtf8()} for table {_settings.TableName}");
#endif
                                AsyncServerStreamingCall<MutateRowsResponse> streamingResponse = BTtestClientPool[GetClientIndex()].SendMutateRowsRequest(request);
                                var task = CheckMutated(streamingResponse);
                                task.Wait();
                            }
                            catch (Exception ex)
                            {
                                Interlocked.Increment(ref failCtr);
                                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Elapsed {(runtime.Elapsed - startTime).TotalMilliseconds} milliseconds\n Exception inserting record {startId} last rowkey {rowKey.ToStringUtf8()} for table {_settings.TableName}, error message: {ex.Message}");
                            }
                            finally
                            {
                                var endTime = runtime.Elapsed;
                                var responseTime = endTime - startTime;
                                loadHistogram.RecordValue(responseTime.Ticks); // milliseconds * 10000
                            }
                        }
                    }));
                }
                threads.ForEach(a => a.Start());
                threads.ForEach(a => a.Join());
            }
        }

        internal void ReadWriteTest(LongConcurrentHistogram readHistogram, LongConcurrentHistogram writeHistogram)
        {
            readHistogram = new LongConcurrentHistogram(3, TimeStamp.Hours(1), 3);
            writeHistogram = new LongConcurrentHistogram(3, TimeStamp.Hours(1), 3);
            Type = "ReadWriteMultipleChannels";

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Instance: {_settings.InstanceId}");
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Starting {Type} test against Table: {_settings.TableName} for {_settings.RpcTestDurationMinutes} minutes using {_settings.RpcThreads} thread");

            List<Thread> threads = new List<Thread>();

            var runtime = Stopwatch.StartNew();

            for (var i = 0; i < _settings.RpcThreads; i++)
            {
                threads.Add(new Thread(() =>
                {
                    while (runtime.Elapsed.TotalMinutes < _settings.RpcTestDurationMinutes)
                    {
                        int rowNum = GetRandom(_settings.Records);
                        ByteString rowKey = ByteString.CopyFromUtf8(_settings.RowKeyPrefix + rowNum.ToString(_stringFormat));
#if DEBUG
                        Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Reading rowKey {rowKey.ToStringUtf8()} ");
#endif
                        if (rowNum % 2 == 0)
                        {
                            var startReadTime = runtime.Elapsed;

                            var readRowRerquest = new ReadRowsRequest
                            {
                                TableName = _table,
                                Rows = new RowSet { RowKeys = { rowKey}},
                                //RowsLimit = 1,
                                Filter = new RowFilter { CellsPerColumnLimitFilter = 1 },
                            };

                            try
                            {
                                BTtestClientPool[GetClientIndex()].SendReadRowsRequest(readRowRerquest);
                            }
                            catch (Exception ex)
                            {
                                Interlocked.Increment(ref failCtr);
                                Console.WriteLine(
                                    $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Elapsed {(runtime.Elapsed - startReadTime).TotalMilliseconds} milliseconds\n Failed to read rowKey {rowKey.ToStringUtf8()}, error message: {ex.Message}");
                            }
                            finally
                            {
                                var endReadTime = runtime.Elapsed;
                                var responseTime = endReadTime - startReadTime;
                                readHistogram.RecordValue(responseTime.Ticks); // milliseconds * 10000
                            }
                        }
                        else
                        {
                            var mutateRowrequest = new MutateRowRequest
                            {
                                TableName = _table,
                                RowKey = rowKey,
                                Mutations = { MutationsBuilder() }
                            };

#if DEBUG
                            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Inserting rowKey {rowKey.ToStringUtf8()} ");
#endif
                            var startInsertTime = runtime.Elapsed;
                            
                            try
                            {
                                BTtestClientPool[GetClientIndex()].SendMutateRowRequest(mutateRowrequest);
                            }
                            catch (Exception ex)
                            {
                                Interlocked.Increment(ref failCtr);
                                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Elapsed {(runtime.Elapsed - startInsertTime).TotalMilliseconds} milliseconds\nFailed to insert rowKey {rowKey.ToStringUtf8()}, error message: {ex.Message}");
                            }
                            finally
                            {
                                var endInsertTime = runtime.Elapsed;
                                var responseTime = endInsertTime - startInsertTime;
                                writeHistogram.RecordValue(responseTime.Ticks); // milliseconds * 10000
                            }
                        }
                    }
                }));
            }
            threads.ForEach(a => a.Start());
            threads.ForEach(a => a.Join());
        }

        internal void CreateTable(BigtableTableAdminClient bigtableTableAdminClient)
        {
            try
            {
                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Creating table {_settings.TableName} with column family {_settings.ColumnFamily}, Cluster: {_settings.InstanceId}");

                bigtableTableAdminClient.CreateTable(
                    new InstanceName(_settings.ProjectId, _settings.InstanceId),
                    _settings.TableName,
                    new Table
                    {
                        Granularity = Table.Types.TimestampGranularity.Millis,
                        ColumnFamilies =
                        {
                            {
                                _settings.ColumnFamily, new ColumnFamily
                                {
                                    GcRule = new GcRule
                                    {
                                        MaxNumVersions = 1
                                    }
                                }
                            }
                        }
                    });

                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Table {_settings.TableName} created successfull.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Exception while creating table {_settings.TableName}. Error Message: " + ex.Message);
            }

            var x = bigtableTableAdminClient.GetTable(new TableName(_settings.ProjectId, _settings.InstanceId, _settings.TableName));
        }

        internal void FillRandomData()
        {
            Byte[] b = new Byte[_settings.ColumnLength];
            Rand = new ByteString[RandSize];
            for (var i = 0; i < Rand.Length; i++)
            {
                _rnd.NextBytes(b);
                Rand[i] = ByteString.CopyFrom(b);
            }
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

        private void FillChannelPool(ChannelCredentials channelCredentials)
        {
            for (int i = 0; i < ChPoolSize; i++)
            {
                BTtestClientPool.Add(new BigtableTestClient(new Bigtable.BigtableClient(GetChannel(channelCredentials))));
            }
        }

        internal string GetRandomTableName() => _settings.TablePrefix + Path.GetRandomFileName().Substring(0, 8);

        private string SetTableName() => $"projects/grass-clump-479/instances/{_settings.InstanceId}/tables/{_settings.TableName}";

        private int GetRandom(int upperLimit)
        {
            try
            {
                int value;
                lock (lockObj)
                {
                    value = _rnd.Next(0, upperLimit);
                }
                return value;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                throw;
            }
        }

        private Mutation[] MutationsBuilder()
        {
            Mutation[] mutations = new Mutation[_settings.Columns];
            //int rndIndex = GetRandom(RandSize);
            for (int i = 0; i < mutations.Length; i++)
            {
                var mutation = new Mutation
                {
                    SetCell = new Mutation.Types.SetCell
                    {
                        ColumnQualifier = ByteString.CopyFromUtf8($"field{i}"),
                        FamilyName = _settings.ColumnFamily,
                        Value = Rand[GetRandom(RandSize)]
                    }
                };
                mutations[i] = mutation;
            }

            return mutations;
        }

        private async Task CheckMutated(AsyncServerStreamingCall<MutateRowsResponse> response)
        {
            IAsyncEnumerator<MutateRowsResponse> responseStream = response.ResponseStream;
            while (await responseStream.MoveNext())
            {
                var current = responseStream.Current;
#if DEBUG
                _recordsWritten += current.Entries.Count;
                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} task inserted {current.Entries.Count} rows, total {_recordsWritten}");
#endif
            }
        }
        
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

        private int GetClientIndex()
        {
            lock (lockObj)
            {
                return (Interlocked.Increment(ref chNum) - 1) % ChPoolSize;
            }
        }

        internal void DeleteTable(BigtableTableAdminClient bigtableTableAdminClient)
        {
        try
            {
                bigtableTableAdminClient.DeleteTable(
                    new Google.Cloud.Bigtable.Admin.V2.TableName(_settings.ProjectId, _settings.InstanceId, _settings.TableName));

                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Table {_settings.TableName} deleted successfully.");
            }
        catch (Exception ex)
            {
                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Exception while deleting table {_settings.TableName}, error message: {ex.Message}");
            }
        }

    public void WriteCsvToConsole(TimeSpan loadDuration, LongConcurrentHistogram loadHistogram, LongConcurrentHistogram readHistogram, LongConcurrentHistogram writeHistogram)
        {
            try
            {
                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} test is finished.");
                Console.WriteLine(
                    $"{Type} operations={loadHistogram.TotalCount:N0}, throughput={loadHistogram.TotalCount / loadDuration.TotalSeconds:N2}, 50th={loadHistogram.GetValueAtPercentile(50) / 10000d}, 75th={loadHistogram.GetValueAtPercentile(75) / 10000d}, 95th={loadHistogram.GetValueAtPercentile(95) / 10000d}, 99th={loadHistogram.GetValueAtPercentile(99) / 10000d}, 99.9th={loadHistogram.GetValueAtPercentile(99.9) / 10000d}");
                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} test is finished.");
                Console.WriteLine(
                    $"Read operations={readHistogram.TotalCount:N0}, throughput={readHistogram.TotalCount / _settings.RpcTestDurationMinutes * 60d:N2}, 50th={readHistogram.GetValueAtPercentile(50) / 10000d}, 75th={readHistogram.GetValueAtPercentile(75) / 10000d}, 95th={readHistogram.GetValueAtPercentile(95) / 10000d}, 99th={readHistogram.GetValueAtPercentile(99) / 10000d}, 99.9th={readHistogram.GetValueAtPercentile(99.9) / 10000d}");
                Console.WriteLine(
                    $"Write operations={writeHistogram.TotalCount:N0}, throughput={writeHistogram.TotalCount / _settings.RpcTestDurationMinutes * 60d:N2}, 50th={writeHistogram.GetValueAtPercentile(50) / 10000d}, 75th={writeHistogram.GetValueAtPercentile(75) / 10000d}, 95th={writeHistogram.GetValueAtPercentile(95) / 10000d}, 99th={writeHistogram.GetValueAtPercentile(99) / 10000d}, 99.9th={writeHistogram.GetValueAtPercentile(99.9) / 10000d}");

            }
            catch (Exception ex)
            {
                Console.WriteLine(
                    $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Exception writing test results to console, error: {ex.Message}");
            }
        }
    }
}
