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
using System.Data.Common;
using System.Text.RegularExpressions;
using System.Runtime.CompilerServices;
using DataTable = DataLib.Table.Impl.DataTable;
using DataColumnCollection = DataLib.Table.Impl.DataColumnCollection;
using System;
using System.IO;
using static System.Net.WebRequestMethods;

namespace Hdf5Lib.Table.Impl;

public class DBContext : DBContextBasic
{
    private object locker = new object();

    /// <summary>
    /// The deleted rows
    /// </summary>
    private List<int> deletedRows = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="DBContext"/> class.
    /// </summary>
    /// <param name="table">The table.</param>
    public DBContext(IDataTable table) : base(table)
    {
    }

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

        var fileId = System.IO.File.Exists(this.DBPath) ? Hdf5.OpenFile(this.DBPath) : Hdf5.CreateFile(this.DBPath);
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

        var fileId = System.IO.File.Exists(this.DBPath) ? Hdf5.OpenFile(this.DBPath) : Hdf5.CreateFile(this.DBPath);
        var groupId = Hdf5.CreateOrOpenGroup(fileId, rows.Table.OriginalTable);

        if (rows.Table.Columns?.Any() ?? false)
        {
            rows.Table.Columns.ForEach(column =>
            {
                var values = rows.Select(m => m[column.ColumnIndex]).ToArray();
                this.WriteData(groupId, column, values);
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
    public override IDataTable Query(IQuerySetting setting)
    {
        if (setting.Table == null)
            throw new ArgumentNullException(nameof(IQuerySetting.Table));

        if (string.IsNullOrWhiteSpace(setting.Table.OriginalTable))
            throw new ArgumentNullException(nameof(IQuerySetting.Table.OriginalTable));

        if (!(setting.Columns?.Any() ?? false))
            throw new ArgumentNullException(nameof(IQuerySetting.Columns));

        var rowIndexs = new List<int>();
        var h5file = H5File.OpenRead(this.DBPath);
        var h5group = h5file.Group(setting.Table.OriginalTable);
        var table = new DataTable(setting.Table, setting.Columns.Columns);

        // 对查询条件进行过滤
        setting.Parameters.ForEach(p =>
        {
            var h5dataset = h5group.Dataset(p.DataColumn.Field);
            var dataValues = this.ReadData(h5dataset, p.DataColumn);
            var indexs = p.Filter(dataValues);

            if (rowIndexs.Any())
                rowIndexs = (indexs?.Any() ?? false) ? rowIndexs?.Intersect(indexs).ToList() : rowIndexs;
            else
                rowIndexs = (indexs?.Any() ?? false) ? indexs : rowIndexs;
        });

        // 提出已删除的数据
        if (deletedRows?.Any() ?? false)
            rowIndexs = rowIndexs.Except(deletedRows).ToList();

        //按列取出数据
        var rowIndex = 0;
        table.Columns.ForEach(col =>
        {
            var h5dataset = h5group.Dataset(col.Field);
            var dataValues = this.ReadData(h5dataset, col);
            rowIndex = 0;

            foreach (var index in rowIndexs)
            {
                if (table.RowCount > rowIndex)
                {
                    var row = table.Rows[rowIndex];
                    row[col.ColumnIndex] = dataValues.GetValue(index);
                    row.RowIndex = index;
                }
                else
                {
                    var row = table.NewRow();
                    row[col.ColumnIndex] = dataValues.GetValue(index);
                    row.RowIndex = index;
                    table.Rows.Add(row);
                }

                rowIndex++;
            }
        });

        h5file.Dispose();
        return table;
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

        IEnumerable<int> rowIndexs = new List<int>();
        var h5file = H5File.OpenRead(this.DBPath);
        var h5group = h5file.Group(setting.Table.OriginalTable);
        var table = new DataTable(setting.Table, setting.Columns.Columns);

        // 对查询条件进行过滤
        for (int i = 0; i < setting.Parameters.Count; i++)
        {
            var parameter = setting.Parameters[i];
            var h5dataset = h5group.Dataset(parameter.DataColumn.Field);
            var dataValues = await this.ReadDataAsync(h5dataset, parameter.DataColumn);
            var indexs = parameter.Filter(dataValues);

            if (rowIndexs.Any())
                rowIndexs = (indexs?.Any() ?? false) ? rowIndexs?.Intersect(indexs).ToList() : rowIndexs;
            else
                rowIndexs = (indexs?.Any() ?? false) ? indexs : rowIndexs;
        }

        // 提出已删除的数据
        if (deletedRows?.Any() ?? false)
            rowIndexs = rowIndexs.Except(deletedRows).ToList();

        //按列取出数据
        var rowIndex = 0;
        for (int i = 0; i < table.ColumnCount; i++)
        {
            var col = table.Columns[i];
            var h5dataset = h5group.Dataset(col.Field);
            var dataValues = await this.ReadDataAsync(h5dataset, col);
            rowIndex = 0;

            if (rowIndexs?.Any() ?? false)
            {
                foreach (var index in rowIndexs)
                {
                    if (table.RowCount > rowIndex)
                    {
                        var row = table.Rows[rowIndex];
                        row[col.ColumnIndex] = dataValues.GetValue(index);
                        row.RowIndex = index;
                    }
                    else
                    {
                        var row = table.NewRow();
                        row[col.ColumnIndex] = dataValues.GetValue(index);
                        row.RowIndex = index;
                        table.Rows.Add(row);
                    }

                    rowIndex++;
                }
            }
            else
            {
                for (int index = 0; index < dataValues.Length; index++)
                {
                    if (table.RowCount > rowIndex)
                    {
                        var row = table.Rows[rowIndex];
                        row[col.ColumnIndex] = dataValues.GetValue(index);
                        row.RowIndex = index;
                    }
                    else
                    {
                        var row = table.NewRow();
                        row[col.ColumnIndex] = dataValues.GetValue(index);
                        row.RowIndex = index;
                        table.Rows.Add(row);
                    }

                    rowIndex++;
                }
            }
        }

        h5file.Dispose();
        return table;
    }

    /// <summary>
    /// Queries the row count.
    /// </summary>
    /// <param name="setting">The setting.</param>
    /// <returns></returns>
    public override async Task<int> QueryRowCountAsync()
    {
        var count = 0;
        var column = this.DataTable.Columns[0];
        using (var h5file = H5File.OpenRead(this.DBPath))
        {
            var h5group = h5file.Group(this.DataTable.OriginalTable);
            var h5dataset = h5group.Dataset(column.Field);
            var dataValues = await this.ReadDataAsync(h5dataset, column);
            count = dataValues.Length;
        }

        return count;
    }

    /// <summary>
    /// Reads the data.
    /// </summary>
    /// <param name="dataset">The dataset.</param>
    /// <param name="column">The column.</param>
    /// <returns></returns>
    private Array ReadData(long groupId, IDataColumn column)
    {
        switch (column.TypeCode)
        {
            case TypeCode.Boolean:
            case TypeCode.Int16:
            case TypeCode.Int32:
            case TypeCode.UInt16:
            case TypeCode.UInt32:
                return Hdf5.ReadDataset<int>(groupId, column.Field).result;
            case TypeCode.Int64:
            case TypeCode.UInt64:
                return Hdf5.ReadDataset<long>(groupId, column.Field).result;
            case TypeCode.Single:
            case TypeCode.Double:
            case TypeCode.Decimal:
                return Hdf5.ReadDataset<double>(groupId, column.Field).result;
            case TypeCode.DateTime:
            case TypeCode.String:
            default:
                return Hdf5.ReadStrings(groupId, column.Field, string.Empty, false).result?.ToArray();
        }
    }

    /// <summary>
    /// Reads the data asynchronous.
    /// </summary>
    /// <param name="dataset">The dataset.</param>
    /// <param name="column">The column.</param>
    /// <returns></returns>
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
                return await dataset.ReadAsync<double>();
            case TypeCode.DateTime:
            case TypeCode.String:
            default:
                return await dataset.ReadStringAsync();
        }
    }

    /// <summary>
    /// Reads the data.
    /// </summary>
    /// <param name="dataset">The dataset.</param>
    /// <param name="column">The column.</param>
    /// <returns></returns>
    private Array ReadData(H5Dataset dataset, IDataColumn column)
    {
        switch (column.TypeCode)
        {
            case TypeCode.Boolean:
                return dataset.Read<bool>();
            case TypeCode.Int16:
            case TypeCode.Int32:
                return dataset.Read<int>();
            case TypeCode.UInt16:
            case TypeCode.UInt32:
                return dataset.Read<uint>();
            case TypeCode.Int64:
                return dataset.Read<long>();
            case TypeCode.UInt64:
                return dataset.Read<ulong>();
            case TypeCode.Single:
            case TypeCode.Double:
            case TypeCode.Decimal:
                return dataset.Read<double>();
            case TypeCode.DateTime:
            case TypeCode.String:
            default:
                return dataset.ReadString();
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

    /// <summary>
    /// Writes the data.
    /// </summary>
    /// <param name="groupId">The group identifier.</param>
    /// <param name="column">The column.</param>
    /// <param name="array">The array.</param>
    private void WriteData(long groupId, IDataColumn column, Array array)
    {
        Array values = null;

        var index = 0;
        if (column.IsAutoincrement)
            values = Enumerable.Range(0, array.Length).Select(m => index++).ToArray();
        else
        {
            switch (column.TypeCode)
            {
                case TypeCode.Boolean:
                    values = array.ConvertArray<object, int>(m => Convert.ToInt32(m));
                    break;
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                    values = array.ConvertArray<object, int>(m => Convert.ToInt32(m));
                    break;
                case TypeCode.Int64:
                case TypeCode.UInt64:
                    values = array.ConvertArray<object, long>(m => Convert.ToInt64(m));
                    break;
                case TypeCode.Single:
                case TypeCode.Double:
                case TypeCode.Decimal:
                    values = array.ConvertArray<object, double>(m => Convert.ToDouble(m));
                    break;
                case TypeCode.DateTime:
                case TypeCode.String:
                default:
                    values = array.ConvertArray<object, string>(m => $"{m}");
                    break;
            }
        }

        Hdf5.WriteDataset(groupId, column.Field, values);
    }

    /// <summary>
    /// Gets the default array.
    /// </summary>
    /// <param name="column">The column.</param>
    /// <param name="length">The length.</param>
    /// <returns>Array</returns>
    private Array GetDefaultArray(IDataColumn column, int length)
    {
        Array values = null;

        switch (column.TypeCode)
        {
            case TypeCode.Boolean:
            case TypeCode.Int16:
            case TypeCode.Int32:
            case TypeCode.UInt16:
            case TypeCode.UInt32:
                values = Enumerable.Range(0, length).Select(z => 0).ToArray();
                break;
            case TypeCode.Int64:
            case TypeCode.UInt64:
                values = Enumerable.Range(0, length).Select(z => 0L).ToArray();
                break;
            case TypeCode.Single:
            case TypeCode.Double:
            case TypeCode.Decimal:
                values = Enumerable.Range(0, length).Select(z => double.NaN).ToArray();
                break;
            case TypeCode.DateTime:
            case TypeCode.String:
            default:
                values = Enumerable.Range(0, length).Select(z => string.Empty).ToArray();
                break;
        }

        return values;
    }

    protected override async Task OnConfiguring() => await Task.CompletedTask;

    public override void Dispose()
    {
    }

    public override async Task MergeColumnsAsync(IMergeSetting setting)
    {
        var maxColumn = 100;
        var maxRow = 1000000;
        var leftTable = new DataTable(setting.LeftColumns) { DBFile = setting.TableId, OriginalTable = setting.TableName }; // 生产项目需要进行适配， 通过tableId 查找对应的 IDataTable
        var rightTable = new DataTable(setting.RightColumns) { DBFile = setting.RightTableId, OriginalTable = setting.RightTableName }; // 生产项目需要进行适配， 通过tableId 查找对应的 IDataTable

        if (leftTable.ColumnCount > maxColumn)
            throw new Exception($"The left data table exceeds the maximum allowed column number of {maxColumn} columns.");

        if (rightTable.ColumnCount > maxColumn)
            throw new Exception($"The right data table exceeds the maximum allowed column number of {maxColumn} columns.");

        leftTable.StoreRowCount = await leftTable.QueryRowCountAsync();
        if (leftTable.StoreRowCount > maxRow)
            throw new Exception($"The data table on the left exceeds the maximum allowed row number of {maxRow} rows.");

        rightTable.StoreRowCount = await rightTable.QueryRowCountAsync();
        if (rightTable.StoreRowCount > maxRow)
            throw new Exception($"The data table on the right exceeds the maximum allowed row number of {maxRow} rows.");

        var leftQSetting = new QuerySetting() { Columns = setting.LeftColumns, Table = leftTable };
        var rightQSetting = new QuerySetting() { Columns = setting.RightColumns, Table = rightTable };
        var leftData = leftTable.QueryAsync(leftQSetting); // 从左表数据文件中加载数据到内存
        var rightData = rightTable.QueryAsync(rightQSetting); // 从右表数据文件中加载数据到内存
        var leftRows = await leftData;
        var rightRows = await rightData;
        var newTable = new DataTable(leftTable);
        newTable.Columns.AddRange(setting.LeftColumns);
        newTable.Columns.AddRange(setting.RightColumns);

        Func<IDataRow, List<(IDataColumn Left, IDataColumn right)>, bool, string> key = (row, mathColumns, isleft) => isleft ? string.Join("_", mathColumns.Select(m => $"{row[m.Left.Field]}")) : string.Join("_", mathColumns.Select(m => $"{row[m.right.Field]}"));

        Func<IDataTable, IDataRow, IDataRow, IDataColumnCollection, IDataColumnCollection, IDataRow> get = (table, leftRow, rightRow, leftColumns, rightColumns) =>
        {
            var row = table.NewRow();
            leftColumns.ForEach(column => row[column.Field] = leftRow[column.Field]);
            rightColumns.ForEach(column => row[column.Field] = rightRow[column.Field]);
            return row;
        };

        var linq = from lr in leftRows.Rows.Rows
                   join rr in rightRows.Rows.Rows
                   on key(lr, setting.MacthCloumns, true) equals key(rr, setting.MacthCloumns, false)
                   select get(newTable, lr, rr, setting.LeftColumns, setting.RightColumns);

        foreach (var row in linq)
            newTable.Rows.Add(row);

        leftRows.Rows.Rows.Clear();
        rightRows.Rows.Rows.Clear();
        await newTable.InsertAsync(newTable.Rows);
    }

    public override async Task MergeRowsAsync(IUpdateSetting setting) => await Task.CompletedTask;

    public override async Task MergeRowsAsync(IMergeSetting setting)
    {
        var newTable = new DataTable();
        var leftTable = new DataTable(setting.LeftColumns) { DBFile = setting.TableId, OriginalTable = setting.TableName, Name = setting.TableName }; // 生产项目需要进行适配， 通过tableId 查找对应的 IDataTable
        var rightTable = new DataTable(setting.RightColumns) { DBFile = setting.RightTableId, OriginalTable = setting.RightTableName, Name = setting.RightTableName }; // 生产项目需要进行适配， 通过tableId 查找对应的 IDataTable         
        leftTable.StoreRowCount = await leftTable.QueryRowCountAsync();
        newTable.Columns.AddRange(new DataColumnCollection(setting.LeftColumns, newTable));
        newTable.Columns.AddRange(new DataColumnCollection(setting.NewColumns, newTable));

        var leftFileId = Hdf5.OpenFile(leftTable.DBFile);
        var rightFileId = Hdf5.OpenFile(rightTable.DBFile);
        var leftGroup = Hdf5.CreateOrOpenGroup(leftFileId, leftTable.OriginalTable);
        var rightGroup = Hdf5.CreateOrOpenGroup(rightFileId, rightTable.OriginalTable);

        for (int i = 0; i < setting.MacthCloumns.Count; i++)
        {
            var rowIndex = 0;
            var math = setting.MacthCloumns[i];
            var leftValues = this.ReadData(leftGroup, math.Left);
            var rightValues = this.ReadData(rightGroup, math.right);
            var values = new object[leftValues.Length + rightValues.Length];

            for (int li = 0; li < leftValues.Length; li++)
                values[rowIndex++] = leftValues.GetValue(li);

            for (int ri = 0; ri < rightValues.Length; ri++)
                values[rowIndex++] = rightValues.GetValue(ri);

            this.WriteData(leftGroup, math.Left, values);
        }

        if (setting.LeftColumns?.Any() ?? false)
        {
            for (int i = 0; i < setting.LeftColumns.Count; i++)
            {
                var col = setting.LeftColumns[i];

                if (!setting.MacthCloumns.Any(m => m.Left.Field == col.Field))
                {
                    var leftValues = this.ReadData(leftGroup, col);
                    var values = this.GetDefaultArray(col, leftTable.StoreRowCount + leftValues.Length);

                    for (int li = 0; li < leftValues.Length; li++)
                        values.SetValue(leftValues.GetValue(li), li);

                    this.WriteData(leftGroup, col, values);
                }
            }
        }

        if (setting.NewColumns?.Any() ?? false)
        {
            for (int i = 0; i < setting.NewColumns.Count; i++)
            {
                var col = setting.NewColumns[i];
                var rightValues = this.ReadData(rightGroup, col);
                var values = this.GetDefaultArray(col, leftTable.StoreRowCount + rightValues.Length);

                for (int ri = 0; ri < rightValues.Length; ri++)
                    values.SetValue(rightValues.GetValue(ri), ri + leftTable.StoreRowCount);

                this.WriteData(leftGroup, col, values);
            }
        }

        Hdf5.CloseGroup(leftGroup);
        Hdf5.CloseGroup(rightGroup);
        Hdf5.CloseFile(leftFileId);
        Hdf5.CloseFile(rightFileId);
    }

    public override async Task RenameAsync(string table, string rename) => await Task.CompletedTask;

    /// <summary>
    /// Drops table the asynchronous.
    /// </summary>
    /// <param name="table">The table.</param>
    /// <returns>Task</returns>
    /// <exception cref="ArgumentNullException">table</exception>
    public override async Task DropAsync(string table) => await Task.CompletedTask;
}
