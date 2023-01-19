using DataLib.Table.Interfaces;
using DataLib.Table.Impl;
using DataLib.Table;
using HDF5CSharp;
using System.Reflection.Metadata.Ecma335;

namespace Hdf5Lib.Table.Impl;

public class DBContext : DBContextBasic
{
    /// <summary>
    /// The deleted rows
    /// </summary>
    private List<int> deletedRows = new();

    /// <summary>
    /// Adds the columns asynchronous.
    /// </summary>
    /// <param name="setting">The setting.</param>
    /// <exception cref="ArgumentNullException">
    /// Table
    /// or
    /// NewColumns
    /// </exception>
    public override async Task AddColumnsAsync(IUpdateSetting setting)
    {
        if (string.IsNullOrWhiteSpace(setting.Table))
            throw new ArgumentNullException(nameof(IUpdateSetting.Table));

        if (!(setting.NewColumns?.Any() ?? false))
            throw new ArgumentNullException(nameof(IUpdateSetting.NewColumns));

        var fileId = Hdf5.OpenFile(this.DBPath);
        var groupId = Hdf5.CreateOrOpenGroup(fileId, setting.Table);

        if (setting.NewColumns?.Any() ?? false)
        {
            setting.NewColumns.ForEach(column =>
            {
                var values = setting.Rows.Select(m => m[column.ColumnIndex]).ToArray();
                Hdf5.WriteDataset(groupId, column.Field, values);
            });
        }

        await Task.CompletedTask;
    }

    /// <summary>
    /// Creates the table asynchronous.
    /// </summary>
    /// <param name="table">The table.</param>
    /// <exception cref="ArgumentNullException">
    /// table
    /// or
    /// OriginalTable
    /// or
    /// Columns
    /// </exception>
    public override async Task CreateTableAsync(IDataTable table)
    {
        if (table == null)
            throw new ArgumentNullException(nameof(table));

        if (string.IsNullOrWhiteSpace(table.OriginalTable))
            throw new ArgumentNullException(nameof(table.OriginalTable));

        if (!(table.Columns?.Any() ?? false))
            throw new ArgumentNullException(nameof(table.Columns));

        var fileId = File.Exists(this.DBPath) ? Hdf5.OpenFile(this.DBPath) : Hdf5.CreateFile(this.DBPath);
        var groupId = Hdf5.CreateOrOpenGroup(fileId, table.OriginalTable);
        await Task.CompletedTask;
    }

    /// <summary>
    /// Deletes the asynchronous.
    /// </summary>
    /// <param name="setting">The setting.</param>
    /// <returns>Task</returns>
    /// <exception cref="NotImplementedException"></exception>
    public override async Task DelAsync(IUpdateSetting setting)
    {
        if (string.IsNullOrWhiteSpace(setting.Table))
            throw new ArgumentNullException(nameof(IUpdateSetting.Table));

        if (setting.Parameters == null || !setting.Parameters.Any())
            throw new ArgumentNullException(nameof(IUpdateSetting.RowIndexs));

        this.deletedRows.AddRange(setting.RowIndexs);
        await Task.CompletedTask;
    }

    /// <summary>
    /// Inserts the asynchronous.
    /// </summary>
    /// <param name="rows">The rows.</param>
    /// <returns></returns>
    /// <exception cref="ArgumentNullException">
    /// rows
    /// or
    /// Table
    /// or
    /// OriginalTable
    /// </exception>
    public override async Task<int> InsertAsync(IDataRowCollection rows)
    {
        if (!(rows?.Any() ?? false))
            throw new ArgumentNullException(nameof(rows));

        if (rows.Table == null)
            throw new ArgumentNullException(nameof(rows.Table));

        if (string.IsNullOrWhiteSpace(rows.Table.OriginalTable))
            throw new ArgumentNullException(nameof(rows.Table.OriginalTable));

        return await Task.Factory.StartNew(() =>
        {
            var fileId = File.Exists(this.DBPath) ? Hdf5.OpenFile(this.DBPath) : Hdf5.CreateFile(this.DBPath);
            var groupId = Hdf5.CreateOrOpenGroup(fileId, rows.Table.OriginalTable);

            if (rows.Table.Columns?.Any() ?? false)
            {
                rows.Table.Columns.ForEach(column =>
                {
                    var values = rows.Select(m => m[column.ColumnIndex]).ToArray();
                    Hdf5.WriteDataset(groupId, column.Field, values);
                });
            }

            return rows.Count;
        });
    }

    /// <summary>
    /// Updates the asynchronous.
    /// </summary>
    /// <param name="setting">The setting.</param>
    /// <returns></returns>
    /// <exception cref="ArgumentNullException">
    /// Table
    /// or
    /// UpdateColumns
    /// or
    /// PrimaryColumns
    /// </exception>
    public override async Task<int> UpdateAsync(IUpdateSetting setting)
    {
        if (string.IsNullOrWhiteSpace(setting.Table))
            throw new ArgumentNullException(nameof(UpdateSetting.Table));

        if (setting.UpdateColumns == null || !setting.UpdateColumns.Any())
            throw new ArgumentNullException(nameof(UpdateSetting.UpdateColumns));

        if (setting.PrimaryColumns == null || !setting.PrimaryColumns.Any())
            throw new ArgumentNullException(nameof(UpdateSetting.PrimaryColumns));

        if (setting.NewColumns?.Any() ?? false)
            await this.AddColumnsAsync(setting);

        await Task.CompletedTask;
        return 1;
    }

    /// <summary>
    /// Queries the asynchronous.
    /// </summary>
    /// <param name="setting">The setting.</param>
    /// <returns>IDataRowCollection</returns>
    /// <exception cref="NotImplementedException"></exception>
    public override async Task<IDataRowCollection> QueryAsync(IQuerySetting setting)
    {
        if (setting.Table == null)
            throw new ArgumentNullException(nameof(IQuerySetting.Table));

        if (string.IsNullOrWhiteSpace(setting.Table.OriginalTable))
            throw new ArgumentNullException(nameof(IQuerySetting.Table.OriginalTable));

        if (!(setting.Columns?.Any() ?? false))
            throw new ArgumentNullException(nameof(IQuerySetting.Columns));

        return await Task.Factory.StartNew(() =>
        {
            var fileId = Hdf5.OpenFile(this.DBPath);
            var groupId = Hdf5.CreateOrOpenGroup(fileId, setting.Table.OriginalTable);
            var table = new DataTable { OriginalTable = setting.Table.OriginalTable, Name = setting.Table.Name, Id = setting.Table.Id };
            var dicValues = new Dictionary<string, Array>();
            var rowCount = 0;
            setting.Columns.ForEach(column =>
            {
                var newColumn = new DataColumn(column);
                table.Columns.Add(newColumn);
                newColumn.ColumnIndex = table.ColumnCount;
                newColumn.Table = table;
            });

            table.Columns.ForEach(column =>
            {
                var (result, valObjects) = Hdf5.ReadDataset<string>(groupId, column.Field);
                if (result)
                {
                    var values = this.ReadData(valObjects, column);
                    dicValues.Add(column.Field, values);
                    rowCount = values.Length;
                }
            });

            if (dicValues.Count > 0 && rowCount > 0)
            {
                for (int i = 0; i < rowCount; i++)
                {
                    var row = table.NewRow();
                    table.Columns.ForEach(col => row[col.ColumnIndex] = dicValues[col.Field].GetValue(i));
                    table.Rows.Add(row);
                }

                dicValues.Clear();
            }

            return table.Rows;
        });
    }

    /// <summary>
    /// Reads the data.
    /// </summary>
    /// <param name="objects">The objects.</param>
    /// <param name="column">The column.</param>
    /// <returns>Array</returns>
    private Array ReadData(Array objects, IDataColumn column)
    {
        switch (column.TypeCode)
        {
            case TypeCode.Boolean:
                return objects.ConvertArray<string, bool?>(m => bool.TryParse(m, out var val) ? val : null);
            case TypeCode.Int16:
            case TypeCode.UInt16:
            case TypeCode.Int32:
            case TypeCode.UInt32:
                return objects.ConvertArray<string, int?>(m => int.TryParse(m, out var val) ? val : null);
            case TypeCode.Int64:
            case TypeCode.UInt64:
                return objects.ConvertArray<string, long?>(m => long.TryParse(m, out var val) ? val : null);
            case TypeCode.Single:
            case TypeCode.Double:
            case TypeCode.Decimal:
                return objects.ConvertArray<string, double?>(m => double.TryParse(m, out var val) ? val : null);
            case TypeCode.DateTime:
                return objects.ConvertArray<string, DateTime?>(m => DateTime.TryParse(m, out var val) ? val : null);
            case TypeCode.String:
            default:
                return objects;
        }
    }

    protected override async Task OnConfiguring() => await Task.CompletedTask;

    public override void Dispose()
    {
    }

    public override async Task MergeColumnsAsync(IMergeSetting setting) => await Task.CompletedTask;

    public override async Task MergeRowsAsync(IUpdateSetting setting) => await Task.CompletedTask;

    public override async Task MergeRowsAsync(IMergeSetting setting) => await Task.CompletedTask;

    public override async Task RenameAsync(string table, string rename) => await Task.CompletedTask;

    /// <summary>
    /// Drops table the asynchronous.
    /// </summary>
    /// <param name="table">The table.</param>
    /// <returns>Task</returns>
    /// <exception cref="ArgumentNullException">table</exception>
    public override async Task DropAsync(string table) => await Task.CompletedTask;
}
