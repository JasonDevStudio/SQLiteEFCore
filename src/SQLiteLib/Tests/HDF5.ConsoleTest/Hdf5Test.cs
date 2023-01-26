﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Autofac;
using DataLib;
using DataLib.Table;
using DataLib.Table.Impl;
using DataLib.Table.Interfaces;
using HDF5.NET;
using HDF5CSharp;
using Hdf5Lib.Table.Impl;
using Spectre.Console;

namespace HDF5.ConsoleTest;

internal class Hdf5Test
{
    public Hdf5Test()
    {
        Action<ContainerBuilder> build = builder =>
        {
            builder.RegisterType<DBContext>().Keyed<IDBContext>(TableMode.HDF5);
        };


        GlobalService.Register += build;
        GlobalService.Registers();
    }

    public string DBPath { get; set; }

    public int ParaCount { get; set; } = 30;

    public int RowCount { get; set; } = 100;

    public async Task<IDataTable> CreateDataTableAsync(string tableName)
    {
        this.DBPath = string.IsNullOrWhiteSpace(DBPath) ? Path.Combine(@"C:\Users\jiede\Documents\HDF5", $"{DateTime.Now:yyyyMMddHH}.H5") : DBPath;
        if (File.Exists(this.DBPath))
            File.Delete(this.DBPath);

        var st = Stopwatch.StartNew();
        var columns = new DataColumnCollection();
        columns.Add(new DataColumn() { Name = "RowKey", Field = "RowKey", ColumnIndex = columns.Count, VisbleIndex = columns.Count, TypeCode = TypeCode.String });
        columns.Add(new DataColumn() { Name = "WaferId", Field = "WaferId", ColumnIndex = columns.Count, VisbleIndex = columns.Count, TypeCode = TypeCode.String });
        columns.Add(new DataColumn() { Name = "DieX", Field = "DieX", ColumnIndex = columns.Count, VisbleIndex = columns.Count, TypeCode = TypeCode.Int32 });
        columns.Add(new DataColumn() { Name = "DieY", Field = "DieY", ColumnIndex = columns.Count, VisbleIndex = columns.Count, TypeCode = TypeCode.Int32 });
        columns.Add(new DataColumn() { Name = "OrigX", Field = "OrigX", ColumnIndex = columns.Count, VisbleIndex = columns.Count, TypeCode = TypeCode.Int32 });
        columns.Add(new DataColumn() { Name = "OrigY", Field = "OrigY", ColumnIndex = columns.Count, VisbleIndex = columns.Count, TypeCode = TypeCode.Int32 });
        columns.Add(new DataColumn() { Name = "Product", Field = "Product", ColumnIndex = columns.Count, VisbleIndex = columns.Count, TypeCode = TypeCode.String });

        for (int i = 0; i < ParaCount; i++)
        {
            columns.Add(new DataColumn() { Name = $"Para_{i}", Field = $"Para_{i}", ColumnIndex = columns.Count, VisbleIndex = columns.Count, TypeCode = TypeCode.Double });
        }

        var table = await DataTable.CreateTableAsync(tableName, tableName, columns, this.DBPath);


        st.Stop();
        return table;
    }

    public async Task WriteDataTableAsync(IDataTable table)
    {
        var stop = Stopwatch.StartNew();
        var waferId = 1;
        AnsiConsole.Write(new Rule("[Green] Start processing data...[/]").Centered());
        var paraColumns = table.Columns.Where(m => m.Field.StartsWith($"Para_")).OrderBy(m => m.ColumnIndex).ToList();

        for (int i = 0; i < RowCount; i++)
        {
            var row = table.NewRow();
            row["RowKey"] = Guid.NewGuid().ToString();
            row["Product"] = $"Hi3680_{i}";
            row["WaferId"] = $"SDS_{waferId++}";
            row["DieX"] = i;
            row["DieY"] = i + 1;
            row["OrigX"] = i;
            row["OrigY"] = i + 1;

            foreach (var column in paraColumns)
                row[column] = 3.1415926789 * (i + 5);

            table.Rows.Add(row);

            if (waferId > 25)
                waferId = 1;
        }

        stop.Stop();
        AnsiConsole.Write(new Rule($"[Green] Processing data end.column count {table.ColumnCount},row count {table.RowCount}, times {stop.Elapsed.TotalSeconds} s [/]").Centered());

        stop.Restart();
        AnsiConsole.Write(new Rule($"[Green] Start write table to hdf5.... [/]").Centered());
        await table.InsertAsync(table.Rows);
        stop.Stop();
        AnsiConsole.Write(new Rule($"[Green] Write table to hdf5 end, times {stop.Elapsed.TotalSeconds} s, count {table.RowCount}... [/]").Centered());
    }

    public async Task QueryDataAsync(IDataTable table)
    {
        var stop = Stopwatch.StartNew();
        AnsiConsole.Write(new Rule("[Green] Start processing Hdf5QueryFilter ...[/]").Centered());
        this.DBPath = string.IsNullOrWhiteSpace(DBPath) ? Path.Combine(@"C:\Users\jiede\Documents\HDF5", $"{DateTime.Now:yyyyMMddHH}.H5") : DBPath;
        var paraColumns = table.Columns.Where(m => m.Field.StartsWith($"Para_")).OrderBy(m => m.ColumnIndex).ToList();
        var querySetting = new QuerySetting { Table = table };
        querySetting.Columns.Add(table.Columns["WaferId"]);
        querySetting.Columns.Add(table.Columns["DieX"]);
        querySetting.Columns.Add(table.Columns["DieY"]);

        for (int i = 0; i < 30; i++)
            querySetting.Columns.Add(paraColumns[i]);

        var setting = new Hdf5QueryFilter
        {
            Logic = LogicMode.IN,
            DataColumn = table.Columns["WaferId"],
            Value = new string[] { "SDS_3" },
            Binary = LogicMode.AND
        };

        querySetting.Parameters.Add(setting);
        stop.Stop();
        AnsiConsole.Write(new Rule($"[Green] Processing Hdf5QueryFilter end. times {stop.Elapsed.TotalSeconds} s [/]").Centered());
        stop.Restart();
        var datatable = await table.QueryAsync(querySetting);
        stop.Stop();
        AnsiConsole.Write(new Rule($"[Green] query data end. count {datatable.RowCount}, times {stop.Elapsed.TotalSeconds} s [/]").Centered());

        // Create a table
        var actable = new Table();

        // Add some columns
        datatable.Columns.ForEach(col => actable.AddColumn(new TableColumn(col.Field).Centered()));

        // Add some rows
        datatable.Rows.ForEach(row => actable.AddRow(row.Values.Cast<string>().ToArray()));

        // Render the table to the console
        AnsiConsole.Write(actable);
    }
}
