using System;
using System.Threading.Tasks;
using System.Diagnostics;
using System.IO;
using HdrHistogram;
using Microsoft.Extensions.Configuration;
using Mono.Options;

namespace BigtableGrpc
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            #region Load configurations

            var builder = new ConfigurationBuilder().
                SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", false, true);

            IConfigurationRoot configuration = builder.Build();

            BigtableGrpcSettings settings = new BigtableGrpcSettings();
            configuration.Bind(settings);
            #endregion

            #region Commandline Parser
            var showHelp = false;

            var parser = new OptionSet()
            {
                "Options:",
                { "i|instance=", "Bigtable InstanceID name", v => settings.InstanceId = v },
                { "r|rows=", "RowKey spectrum/total rows loaded", v => settings.Records = Convert.ToInt64(v)},
                { "m|minutes=", "Scan test duration minutes", v => settings.ScanTestDurationMinutes = Convert.ToInt32(v)},
                { "b|batch=", "ReadRows batch size rows", v => settings.RowsLimit = Convert.ToInt64(v)},
                { "t|table=", "Table name", v => settings.TableName = v},
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

            LongConcurrentHistogram testHistogram = new LongConcurrentHistogram(3, TimeStamp.Hours(1), 3);

            BigtableGrpcScanTest runner = new BigtableGrpcScanTest(settings);

            var stopWatch = Stopwatch.StartNew();

            var rowCount = await runner.Scan(testHistogram);

            stopWatch.Stop();

            runner.WriteCsvToConsole(stopWatch.Elapsed, rowCount, testHistogram);

            runner.WriteCsv(stopWatch.Elapsed, rowCount, testHistogram);
        }
    }
}
