using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using HdrHistogram;
using Microsoft.Extensions.Configuration;
using Mono.Options;
using Google.Api.Gax.Grpc;
using Google.Cloud.Bigtable.Admin.V2;
using Grpc.Core;

namespace ReadWriteGrpc
{
    class Program
    {
        public static void Main(string[] args)
        {
            #region Load configurations

            var builder = new ConfigurationBuilder().
                SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", false, true);

            IConfigurationRoot configuration = builder.Build();

            ReadWriteGrpcSettings settings = new ReadWriteGrpcSettings();
            configuration.Bind(settings);
            #endregion

            #region Commandline Parser
            var showHelp = false;

            var parser = new OptionSet()
            {
                "Options:",
                { "i|instance=", "Bigtable InstanceID name", v => settings.InstanceId = v },
                { "r|rows=", "RowKey spectrum/total rows loaded", v => settings.Records = Convert.ToInt32(v)},
                { "m|minutes=", "Test duration minutes", v => settings.RpcTestDurationMinutes = Convert.ToInt32(v)},
                { "e|rpc=", "Number of RPC threads", v => settings.RpcThreads = Convert.ToInt32(v)},
                { "T|Table=", "Table name", v => settings.TableName = v},
                { "n|number=", "number of gRPC channels", v => settings.Channels = Convert.ToInt32(v)},
                { "H|Help", "Show this message and exit", v => showHelp = v != null },
            };

            try
            {
                parser.Parse(args);
            }
            catch (OptionException ex)
            {
                Console.WriteLine(ex.Message);
                Console.WriteLine("Try `-H|--Help' for more information.");
                parser.WriteOptionDescriptions(Console.Out);
                return;
            }

            if (showHelp)
            {
                parser.WriteOptionDescriptions(Console.Out);
                return;
            }
            #endregion

            LongConcurrentHistogram loadTestHistogram = new LongConcurrentHistogram(3, TimeStamp.Hours(1), 3);
            LongConcurrentHistogram readTestHistogram = new LongConcurrentHistogram(3, TimeStamp.Hours(1), 3);
            LongConcurrentHistogram writeTestHistogram = new LongConcurrentHistogram(3, TimeStamp.Hours(1), 3);
            BigtableTableAdminClient bigtableTableAdminClient = BigtableTableAdminClient.Create();


            ReadWriteGrpcTest runner = new ReadWriteGrpcTest(settings);
            runner.FillRandomData();
            settings.TableName = runner.GetRandomTableName();
            runner.CreateTable(bigtableTableAdminClient);

            var stopWatch = Stopwatch.StartNew();

            runner.LoadTest(loadTestHistogram);

            stopWatch.Stop();

            runner.ReadWriteTest(readTestHistogram, writeTestHistogram);

            runner.WriteCsvToConsole(stopWatch.Elapsed, loadTestHistogram, readTestHistogram, writeTestHistogram);

            runner.DeleteTable(bigtableTableAdminClient);
        }
    }
}
