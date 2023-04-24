using System.Diagnostics;
using System.Runtime;
using Spectre.Console;

namespace HDF5.ConsoleTest
{
    internal class Program
    {
        public static int RowCount = 1000000;
        public static int ParaCount { get; set; } = 100;

        static async Task Main(string[] args)
        {
            GCSettings.LatencyMode = GCLatencyMode.Batch;
            // WriteLongTable();
            // WriteStringLongTable();
            // await QueryStringData();
            // QueryDataset();
            // await QueryNumberData2();
            // WriteLong2DTable();

            var tester = new DuckDBTest() { RowCount = RowCount, ParaCount = ParaCount };
            var table = await tester.CreateDataTableAsync("HDF5_TABLE_TEST");
            await tester.WriteDataTableAsync(table);
            await tester.QueryDataAsync(table);

            // await tester.MergeColumnsTest(); // Merge Columns Test
            // await tester.MergeRowsTest(); // Merge Rows Test

            AnsiConsole.Ask<string>("input any key exit.");
        }

        static async Task Main11(string[] args)
        {
            GCSettings.LatencyMode = GCLatencyMode.Batch;
            var tester = new DuckDBTest();
            var stop = Stopwatch.StartNew();
            var stype = new Style(foreground: Color.Orange1);
            AnsiConsole.Write(new FigletText("DuckDB Lib Test").Centered().Color(Color.Red));
            var tableName = "WAT_PARA_LONG"; // AnsiConsole.Ask<int>("input table name:");
            tester.ParaCount = AnsiConsole.Ask<int>("input parameter count:");
            tester.RowCount = AnsiConsole.Ask<int>("input row count:");
            AnsiConsole.Write(new Rule($"[White]Create Table {tableName} [/]").Centered());
            var mainTable = await tester.CreateDataTableAsync(tableName);
            AnsiConsole.Write(new Rule().Centered());
            AnsiConsole.Write(new Rule($"[White]Write Data {tableName} [/]").Centered());
            await tester.WriteDataTableAsync(mainTable);
            AnsiConsole.Write(new Rule().Centered());

            stop.Stop();
            AnsiConsole.Write(new Rule($"[White]Sqlite Lib Test Times {stop.Elapsed.TotalSeconds} s[/]").Centered());
            AnsiConsole.Ask<string>("input any key exit.");
        }
    }
}