using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using Autofac;
using Autofac.Core;
using DataLib.Table.Interfaces;

namespace DataLib.Table.Impl
{
    /// <summary>
    /// IDataTable
    /// </summary>
    public partial class DataTable : IDataTable
    {
        #region 属性，成员

        /// <summary>
        /// 数据库文件地址
        /// </summary>
        public static string DBPath { get; set; }

        /// <summary>
        /// 数据上下文
        /// </summary>
        public IDBContext Context { get; set; }

        /// <summary>
        /// Id
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// Name
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// 本地数据库表名
        /// </summary>
        public string OriginalTable { get; set; }

        /// <summary>
        /// Gets or sets the database file.
        /// </summary>
        /// <value>
        /// The database file.
        /// </value>
        public string DBFile
        {
            get => this.Context?.DBPath;
            set => this.Context.DBPath = value;
        }

        /// <summary>
        /// Gets or sets the mode.
        /// </summary>
        /// <value>
        /// The mode.
        /// </value>
        public TableMode Mode { get; set; } = TableMode.HDF5;

        /// <summary>
        /// 数据行数量
        /// </summary>
        public int RowCount => this.Rows?.Count ?? 0;

        /// <summary>
        /// Gets or sets the store row count.
        /// </summary>
        /// <value>
        /// The store row count.
        /// </value>
        public int StoreRowCount { get; set; }

        /// <summary>
        /// 数据列数量
        /// </summary>
        public int ColumnCount => this.Columns?.Count ?? 0;

        /// <summary>
        /// 数据列集合
        /// </summary>
        public IDataColumnCollection Columns { get; set; }

        /// <summary>
        /// 数据行集合
        /// </summary>
        [Newtonsoft.Json.JsonIgnore]
        [System.Text.Json.Serialization.JsonIgnore]
        public IDataRowCollection Rows { get; set; }

        /// <summary>
        /// 获取数据行
        /// </summary>
        /// <param name="index">数据行索引</param>
        /// <returns>IDataRow</returns>
        public IDataRow this[int index] => this.Rows[index];

        #endregion 属性，成员

        /// <summary>
        /// 构造函数
        /// </summary>
        public DataTable()
        {
            var pataName = "table";
            var parameter = new NamedParameter(pataName, this);
            this.Context = GlobalService.GetService<IDBContext>(this.Mode, parameter);
            this.Id = Guid.NewGuid().ToString();
            this.Rows = new DataRowCollection();
            this.Columns = new DataColumnCollection();
            this.Rows.Table = this;
            this.Columns.Table = this;
        }

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="source">IDataTable</param>
        public DataTable(IDataTable source) : this(source, null)
        {
            this.Id = source.Id; 
            this.DBFile = source.DBFile;
            this.OriginalTable = source.OriginalTable;
            this.Name = source.Name;
            this.Columns.AddRange(source.Columns);
        }

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="source">IDataTable</param>
        /// <param name="columns">List{IDataColumn}</param>
        public DataTable(IDataTable source, List<IDataColumn> columns) : this()
        {
            this.Id = source.Id;
            this.Name = Name;
            this.OriginalTable = source.OriginalTable;
            this.Columns = new DataColumnCollection((columns?.Any() ?? false) ? columns.Select(m => new DataColumn(m)).ToList<IDataColumn>() : source.Columns.Select(m => new DataColumn(m)).ToList<IDataColumn>(), this);

            for (int i = 0; i < this.ColumnCount; i++)
            {
                var col = this.Columns[i];
                col.OriginTableId = this.Id;
                col.Table = this;
                col.ColumnIndex = i;
            }
        }

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="dataColumns">data columns</param>
        public DataTable(IDataColumnCollection dataColumns) : this()
        {
            this.Columns = new DataColumnCollection(dataColumns, this);
        }

        #region Methods

        /// <summary>
        /// 创建数据表
        /// </summary>
        /// <param name="tableName">数据表明</param>
        /// <param name="originalTable">对应本地Sqlite 数据表明</param>
        /// <param name="columns">数据列集合</param>
        /// <param name="dbfile">数据文件</param>
        /// <returns>IDataTable</returns>
        public static async Task<IDataTable> CreateTableAsync(string tableName, string originalTable, IDataColumnCollection columns, string dbfile)
        {
            var table = new DataTable();
            table.Id = Guid.NewGuid().ToString();
            table.Name = tableName;
            table.OriginalTable = originalTable;
            table.Columns = columns;
            table.DBFile = dbfile;

            foreach (var col in columns.Columns)
            {
                col.OriginTableId = table.Id;
                col.Table = table;
            }

            await table.Context.CreateTableAsync(table);

            return table;
        }

        /// <summary>
        /// 创建数据行
        /// </summary>
        /// <returns>IDataRow</returns>
        public IDataRow NewRow()
        {
            var row = new DataRow();
            row.Table = this;
            row.RowIndex = this.RowCount;
            row.Values = new object[this.ColumnCount];
            return row;
        }

        /// <summary>
        /// 刷新图表
        /// </summary>
        public async Task ReflashAsync()
        {
            // todo: 调用 DataSource 刷新数据
            await Task.Factory.StartNew(() => Console.WriteLine("调用 DataSource 刷新数据"));
        }

        /// <summary>
        /// 批量写入数据库
        /// </summary>
        /// <returns>Task</returns>
        public async Task WriteAsync() => await this.Context.InsertAsync(this.Rows);

        /// <summary>
        /// 批量写入数据库
        /// </summary>
        /// <param name="rows">需要写入数据库的数据行集合</param>
        /// <returns>Task</returns>
        public async Task InsertAsync(IDataRowCollection rows) => await this.Context.InsertAsync(rows);

        /// <summary>
        /// 新增数据列
        /// </summary>
        /// <param name="setting">UpdateSetting</param>
        /// <returns>Task</returns>
        public async Task AddColumnsAsync(IUpdateSetting setting)
        {
            await this.Context.AddColumnsAsync(setting);

            foreach (var col in setting.NewColumns.Columns)
            {
                if (!this.Columns.Any(m => m.Name == col.Name && m.Field == col.Field))
                    this.Columns.Add(col);
            }
        }

        /// <summary>
        /// 删除数据
        /// </summary>
        /// <param name="setting">UpdateSetting</param>
        /// <returns>Task</returns>
        public async Task DelAsync(IUpdateSetting setting) => await this.Context.DelAsync(setting);

        /// <summary>
        /// 批量写入数据库
        /// </summary>
        /// <param name="columns">新增的数据列集合</param>
        /// <param name="rows">需要写入数据库的数据行集合</param>
        /// <returns></returns>
        public async Task UpdateAsync(IUpdateSetting setting) => await this.Context.UpdateAsync(setting);

        /// <summary>
        /// 合并行
        /// </summary>
        /// <param name="setting">UpdateSetting</param>
        /// <returns>Task</returns>
        /// <exception cref="ArgumentNullException">UpdateSetting.Table, UpdateSetting.Rows</exception>
        public async Task MergeRowsAsync(IUpdateSetting setting) => await this.Context.MergeRowsAsync(setting);

        /// <summary>
        /// 合并列
        /// </summary>
        /// <param name="setting">MergeSetting</param>
        /// <returns>Task</returns>
        public async Task MergeColumnsAsync(IMergeSetting setting) => await this.Context.MergeColumnsAsync(setting);

        /// <summary>
        /// 合并行
        /// </summary>
        /// <param name="setting">MergeSetting</param>
        /// <returns>Task</returns>
        /// <exception cref="ArgumentNullException">UpdateSetting.Table, UpdateSetting.Rows</exception>
        public async Task MergeRowsAsync(IMergeSetting setting) => await this.Context.MergeRowsAsync(setting);

        /// <summary>
        /// 删除数据表
        /// </summary>
        /// <param name="table">需要删除的数据表名</param>
        /// <returns>Task</returns>
        public async Task DropAsync(string table = null) => await this.Context.DropAsync(table ?? this.OriginalTable);

        /// <summary>
        /// 数据表重命名
        /// </summary>
        /// <param name="table">需要删除的数据表名</param>
        /// <returns>Task</returns>
        public async Task RenameAsync(string rename, string originName = null)
        {
            await this.Context.RenameAsync(originName ?? this.OriginalTable, rename);
            this.OriginalTable = rename;
        }

        /// <summary>
        /// 查询数据
        /// </summary>
        /// <param name="setting">QuerySetting</param>
        /// <returns>IDataRowCollection</returns>
        public async Task<IDataTable> QueryAsync(IQuerySetting setting) => await this.Context.QueryAsync(setting);

        /// <summary>
        /// 查询数据
        /// </summary>
        /// <param name="setting">QuerySetting</param>
        /// <returns>IDataRowCollection</returns>
        public IDataTable Query(IQuerySetting setting) => this.Context.Query(setting);

        /// <summary>
        /// 查询数据
        /// </summary> 
        /// <returns>IDataRowCollection</returns>
        public async Task<int> QueryRowCountAsync() => await this.Context.QueryRowCountAsync();

        /// <summary>
        /// Executes the non query asynchronous.
        /// </summary>
        /// <param name="sql">The SQL.</param>
        /// <exception cref="ArgumentNullException">sql</exception>
        public async Task ExecuteNonQueryAsync(string sql) => await this.Context.ExecuteNonQueryAsync(sql);

        #endregion Methods
    }
}