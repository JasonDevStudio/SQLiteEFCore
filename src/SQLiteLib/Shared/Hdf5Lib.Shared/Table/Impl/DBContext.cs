using DataLib.Table.Interfaces;
using DataLib.Table.Impl;
using DataLib.Table;
using HDF5CSharp;
using System.Reflection.Metadata.Ecma335;
using HDF.PInvoke;
using HDF5.NET;
using System.Runtime.InteropServices;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace Hdf5Lib.Table.Impl;

public class DBContext : DBContextBasic
{
    private object locker = new object();

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

        Hdf5.CloseGroup(groupId);
        Hdf5.CloseFile(fileId);
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
        Hdf5.CloseGroup(groupId);
        Hdf5.CloseFile(fileId);
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

        var fileId = File.Exists(this.DBPath) ? Hdf5.OpenFile(this.DBPath) : Hdf5.CreateFile(this.DBPath);
        var groupId = Hdf5.CreateOrOpenGroup(fileId, rows.Table.OriginalTable);

        if (rows.Table.Columns?.Any() ?? false)
        {
            rows.Table.Columns.ForEach(column =>
            {
                var values = rows.Select(m => m[column.ColumnIndex]?.ToString()).ToArray();
                Hdf5.WriteDataset(groupId, column.Field, values);
            });
        }

        Hdf5.CloseGroup(groupId);
        Hdf5.CloseFile(fileId);
        await Task.CompletedTask;
        return rows.Count;
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
        await Task.CompletedTask;
        return 1;
    }

    /// <summary>
    /// Queries the asynchronous.
    /// </summary>
    /// <param name="setting">The setting.</param>
    /// <returns>IDataRowCollection</returns>
    /// <exception cref="NotImplementedException"></exception>
    public override async Task<IDataTable> QueryAsync(IQuerySetting setting)
    {
        if (setting.Table == null)
            throw new ArgumentNullException(nameof(IQuerySetting.Table));

        if (string.IsNullOrWhiteSpace(setting.Table.OriginalTable))
            throw new ArgumentNullException(nameof(IQuerySetting.Table.OriginalTable));

        if (!(setting.Columns?.Any() ?? false))
            throw new ArgumentNullException(nameof(IQuerySetting.Columns));

        IEnumerable<int> rowIndexs = null;
        using var h5file = H5File.OpenRead(this.DBPath);
        var h5group = h5file.Group(setting.Table.OriginalTable);
        var table = new DataTable(setting.Table, setting.Columns.Columns);

        // 对查询条件进行过滤
        setting.Parameters.ForEach(async p =>
        {
            var h5dataset = h5group.Dataset(p.DataColumn.Field);
            var dataValues = await this.ReadDataAsync(h5dataset, p.DataColumn);
            var indexs = p.Filter(dataValues);
            rowIndexs = rowIndexs.Intersect(indexs);
        });

        // 提出已删除的数据
        rowIndexs = rowIndexs.Except(deletedRows);

        //按列取出数据
        setting.Columns.ForEach(async col =>
        {
            var h5dataset = h5group.Dataset(col.Field);
            var dataValues = await this.ReadDataAsync(h5dataset, col);

            foreach (var index in rowIndexs)
            {
                var row = table.RowCount > index ? table.Rows[index] : table.NewRow();
                row[col.ColumnIndex] = dataValues.GetValue(index);
                row.RowIndex = index;
                table.Rows.Add(row);
            }
        });

        h5file.Dispose();
        return table;
    }

    private async Task<Array> ReadDataAsync(H5Dataset dataset, IDataColumn column)
    {
        switch (column.TypeCode)
        {
            case TypeCode.Boolean:
                return await dataset.ReadAsync<bool>();
            case TypeCode.Int16:
            case TypeCode.Int32:
                return await dataset.ReadAsync<int>();
            case TypeCode.UInt16:
            case TypeCode.UInt32:
                return await dataset.ReadAsync<uint>();
            case TypeCode.Int64:
                return await dataset.ReadAsync<long>();
            case TypeCode.UInt64:
                return await dataset.ReadAsync<ulong>();
            case TypeCode.Single:
            case TypeCode.Double:
            case TypeCode.Decimal:
                return await dataset.ReadAsync<ulong>();
            case TypeCode.DateTime:
            case TypeCode.String:
            default:
                return await dataset.ReadStringAsync();
        }
    }

    /// <summary>
    /// Reads the data.
    /// </summary>
    /// <param name="objects">The objects.</param>
    /// <param name="column">The column.</param>
    /// <returns>Array</returns>
    private async Task<Array> ReadDataAsync(Array objects, IDataColumn column)
    {
        return await Task.Factory.StartNew(() =>
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
        });
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
