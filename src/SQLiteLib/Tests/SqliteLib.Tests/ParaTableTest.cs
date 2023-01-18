using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Spectre.Console;
using SQLiteLib;
using SQLiteLib.Table.Interfaces;
using DataColumn = SQLiteLib.Table.Impl.DataColumn;
using DataColumnCollection = SQLiteLib.Table.Impl.DataColumnCollection;
using DataRowCollection = SQLiteLib.Table.Impl.DataRowCollection;
using DataTable = SQLiteLib.Table.Impl.DataTable;
using Rule = Spectre.Console.Rule;

namespace SqliteLib.Tests;

internal class ParaTableTest
{
    public int RowCount = 100000;
    public int ParaCount { get; set; } = 1000;
    public int ChunkCount = 50000;
    public string DBPath { get; set; } = Path.Combine(AppContext.BaseDirectory, "Data", "Sqlite.db");
    private static int ParaIndex = 1;

    public async Task<IDataTable> CrateParaLongTableAsync(string tableName)
    {
        var stop = Stopwatch.StartNew();
        DataTable.DBPath = DBPath;
        AnsiConsole.MarkupLine("[Green] Start processing data...[/]");
        var columns = new DataColumnCollection();
        columns.Add(new DataColumn() { Name = "RowIndex", Field = "RowIndex", IsPK = true, IsAutoincrement = false, ColumnIndex = columns.Count, VisbleIndex = columns.Count, TypeCode = TypeCode.Int32 });
        columns.Add(new DataColumn() { Name = "RowKey", Field = "RowKey", ColumnIndex = columns.Count, VisbleIndex = columns.Count, TypeCode = TypeCode.String });
        columns.Add(new DataColumn() { Name = "WaferId", Field = "WaferId", ColumnIndex = columns.Count, VisbleIndex = columns.Count, TypeCode = TypeCode.String });
        columns.Add(new DataColumn() { Name = "DieX", Field = "DieX", ColumnIndex = columns.Count, VisbleIndex = columns.Count, TypeCode = TypeCode.Int32 });
        columns.Add(new DataColumn() { Name = "DieY", Field = "DieY", ColumnIndex = columns.Count, VisbleIndex = columns.Count, TypeCode = TypeCode.Int32 });
        columns.Add(new DataColumn() { Name = "OrigX", Field = "OrigX", ColumnIndex = columns.Count, VisbleIndex = columns.Count, TypeCode = TypeCode.Int32 });
        columns.Add(new DataColumn() { Name = "OrigY", Field = "OrigY", ColumnIndex = columns.Count, VisbleIndex = columns.Count, TypeCode = TypeCode.Int32 });
        columns.Add(new DataColumn() { Name = "Product", Field = "Product", ColumnIndex = columns.Count, VisbleIndex = columns.Count, TypeCode = TypeCode.String });

        for (int i = 0; i < ParaCount; i++)
        {
            columns.Add(new DataColumn() { Name = $"Para_{ParaIndex}", Field = $"Para_{ParaIndex}", ColumnIndex = columns.Count, VisbleIndex = columns.Count, TypeCode = TypeCode.Double });
            ParaIndex++;
        }

        var table = await DataTable.CreateTableAsync(tableName, tableName, columns);

        stop.Stop();
        AnsiConsole.MarkupLine($"[Green] End processing data, times {stop.Elapsed.TotalSeconds} s..[/]");
        return table;
    }

    public async Task WriteParaLongTableAsync(IDataTable table)
    {
        var stop = Stopwatch.StartNew();
        var waferId = 1;
        AnsiConsole.MarkupLine("[Green] Start processing data...[/]");
        var paraColumns = table.Columns.Where(m => m.Field.StartsWith($"Para_")).OrderBy(m => m.ColumnIndex).ToList();

        for (int i = 0; i < RowCount; i++)
        {
            var row = table.NewRow();
            row["RowIndex"] = table.Rows.Count;
            row["RowKey"] = Guid.NewGuid().ToString();
            row["Product"] = $"Hi3680_{i}";
            row["WaferId"] = $"SDS_{waferId++}";
            row["DieX"] = i;
            row["DieY"] = i + 1;
            row["OrigX"] = i;
            row["OrigY"] = i + 1;

            foreach (var column in paraColumns)
                row[column] = 3.1415926 * (i + 5);

            table.Rows.Add(row);

            if (waferId > 50)
                waferId = 1;
        }

        stop.Stop();
        AnsiConsole.MarkupLine($"[Green] Processing data end.column count {table.ColumnCount},row count {table.RowCount}, times {stop.Elapsed.TotalSeconds} s [/]");

        stop.Restart();
        AnsiConsole.MarkupLine($"[Green] Start write table to sqlite.... [/]");

        var arry = table.Rows.Rows.Chunk(ChunkCount);

        foreach (var item in arry)
        {
            var rows = new DataRowCollection();
            rows.AddRange(item);
            rows.Table = table;
            await table.InsertAsync(rows);
        }

        stop.Stop();
        AnsiConsole.MarkupLine($"[Green] Write table to sqlite end, times {stop.Elapsed.TotalSeconds} s, count {table.RowCount}... [/]");
    }

    public async Task<IDataTable> CreateParaMainTableTestAsync(string tableName)
    {
        var stop = Stopwatch.StartNew();
        DataTable.DBPath = DBPath;
        AnsiConsole.MarkupLine("[Green] Start processing data...[/]");
        var columns = new DataColumnCollection();
        columns.Add(new DataColumn() { Name = "RowIndex", Field = "RowIndex", IsPK = true, IsAutoincrement = true, ColumnIndex = columns.Count, VisbleIndex = columns.Count, TypeCode = TypeCode.Int32 });
        columns.Add(new DataColumn() { Name = "RowKey", Field = "RowKey", ColumnIndex = columns.Count, VisbleIndex = columns.Count, TypeCode = TypeCode.String });
        columns.Add(new DataColumn() { Name = "WaferId", Field = "WaferId", ColumnIndex = columns.Count, VisbleIndex = columns.Count, TypeCode = TypeCode.String });
        columns.Add(new DataColumn() { Name = "DieX", Field = "DieX", ColumnIndex = columns.Count, VisbleIndex = columns.Count, TypeCode = TypeCode.Int32 });
        columns.Add(new DataColumn() { Name = "DieY", Field = "DieY", ColumnIndex = columns.Count, VisbleIndex = columns.Count, TypeCode = TypeCode.Int32 });
        columns.Add(new DataColumn() { Name = "OrigX", Field = "OrigX", ColumnIndex = columns.Count, VisbleIndex = columns.Count, TypeCode = TypeCode.Int32 });
        columns.Add(new DataColumn() { Name = "OrigY", Field = "OrigY", ColumnIndex = columns.Count, VisbleIndex = columns.Count, TypeCode = TypeCode.Int32 });
        columns.Add(new DataColumn() { Name = "Product", Field = "Product", ColumnIndex = columns.Count, VisbleIndex = columns.Count, TypeCode = TypeCode.String });
        var table = await DataTable.CreateTableAsync(tableName, tableName, columns);

        stop.Stop();
        AnsiConsole.MarkupLine($"[Green] End processing data, times {stop.Elapsed.TotalSeconds} s..[/]");
        return table;
    }

    public async Task WriteParaMainTableAsync(IDataTable table)
    {
        var stop = Stopwatch.StartNew();
        var waferId = 1;
        AnsiConsole.MarkupLine("[Green] Start processing data...[/]");
        var paraColumns = table.Columns.Where(m => m.Field.StartsWith($"Para_")).OrderBy(m => m.ColumnIndex).ToList();

        for (int i = 0; i < RowCount; i++)
        {
            var row = table.NewRow();
            row["RowIndex"] = table.Rows.Count;
            row["RowKey"] = Guid.NewGuid().ToString();
            row["Product"] = $"Hi3680_{i}";
            row["WaferId"] = $"SDS_{waferId++}";
            row["DieX"] = i;
            row["DieY"] = i + 1;
            row["OrigX"] = i;
            row["OrigY"] = i + 1;

            table.Rows.Add(row);

            if (waferId > 50)
                waferId = 1;
        }

        stop.Stop();
        AnsiConsole.MarkupLine($"[Green] Processing data end. count {table.RowCount}, times {stop.Elapsed.TotalSeconds} s [/]");

        stop.Restart();
        AnsiConsole.MarkupLine($"[Green] Start write table to sqlite.... [/]");

        var arry = table.Rows.Rows.Chunk(ChunkCount);

        foreach (var item in arry)
        {
            //AnsiConsole.MarkupLine($"[Green] {DateTime.Now} Start Insert table to sqlite.... [/]");
            var rows = new DataRowCollection { Table = table };
            rows.AddRange(item);
            await table.InsertAsync(rows);
            //AnsiConsole.MarkupLine($"[Green] {DateTime.Now} End Insert table to sqlite.... [/]");
        }

        stop.Stop();
        AnsiConsole.MarkupLine($"[Green] Write table to sqlite end, times {stop.Elapsed.TotalSeconds} s, count {table.RowCount}... [/]");
    }

    public async Task<IDataTable> CreateParaDetailTableTestAsync(string tableName)
    {
        var stop = Stopwatch.StartNew();
        DataTable.DBPath = DBPath;
        AnsiConsole.MarkupLine("[Green] Start processing data...[/]");
        var columns = new DataColumnCollection();
        columns.Add(new DataColumn() { Name = "RowIndex", Field = "RowIndex", IsPK = true, IsAutoincrement = true, ColumnIndex = columns.Count, VisbleIndex = columns.Count, TypeCode = TypeCode.Int32 });

        for (int i = 0; i < ParaCount; i++)
        {
            columns.Add(new DataColumn() { Name = $"Para_{ParaIndex}", Field = $"Para_{ParaIndex}", ColumnIndex = columns.Count, VisbleIndex = columns.Count, TypeCode = TypeCode.Double });
            ParaIndex++;
        }

        var table = await DataTable.CreateTableAsync(tableName, tableName, columns);

        stop.Stop();
        AnsiConsole.MarkupLine($"[Green] End processing data, times {stop.Elapsed.TotalSeconds} s..[/]");
        return table;
    }

    public async Task WriteParaDetailAsync(IDataTable table)
    {
        var stop = Stopwatch.StartNew();
        AnsiConsole.MarkupLine("[Green] Start processing data...[/]");
        var paraColumns = table.Columns.Where(m => m.Field.StartsWith($"Para_")).OrderBy(m => m.ColumnIndex).ToList();

        for (int i = 0; i < RowCount; i++)
        {
            var row = table.NewRow();
            row["RowIndex"] = table.Rows.Count;

            foreach (var column in paraColumns)
                row[column] = 3.1415926 * (i + 5);

            table.Rows.Add(row);
        }

        stop.Stop();
        AnsiConsole.MarkupLine($"[Green] Processing data end. count {table.RowCount}, times {stop.Elapsed.TotalSeconds} s [/]");

        stop.Restart();
        AnsiConsole.MarkupLine($"[Green] Start write table to sqlite.... [/]");

        var arry = table.Rows.Rows.Chunk(ChunkCount);

        foreach (var item in arry)
        {
            //AnsiConsole.MarkupLine($"[Green] {DateTime.Now} Start Insert table to sqlite.... [/]");
            var rows = new DataRowCollection { Table = table };
            rows.AddRange(item);
            await table.InsertAsync(rows);
            //AnsiConsole.MarkupLine($"[Green] {DateTime.Now} End Insert table to sqlite.... [/]");
        }

        stop.Stop();
        AnsiConsole.MarkupLine($"[Green] Write table to sqlite end, times {stop.Elapsed.TotalSeconds} s, count {table.RowCount}... [/]");
    }

    public async Task CreateParaViewAsync(IDataTable mainTable, string viewName)
    {
        var sqlBuilder = new StringBuilder();
        sqlBuilder.Append($"CREATE VIEW WAT_PARA_VIEW AS SELECT {mainTable.SqliteTable}.*");

        for (int i = 0; i < 50; i++)
            sqlBuilder.Append($",WAT_PARA_{i}.*");

        sqlBuilder.Append($"FROM {mainTable.SqliteTable} ");

        for (int i = 0; i < 100; i++)
            sqlBuilder.Append($" INNER JOIN WAT_PARA_{i} ON {mainTable.SqliteTable}.RowIndex = WAT_PARA_{i}.RowIndex");

        var sql = sqlBuilder.ToString();
        await mainTable.ExecuteNonQueryAsync(sql);
    }
}
