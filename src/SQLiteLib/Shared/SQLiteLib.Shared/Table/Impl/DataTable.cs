using System.Windows.Markup;
using SQLiteLib.Table.Interfaces;

namespace SQLiteLib.Table.Impl
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
        public DBContext Context { get; set; }

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
        public string SqliteTable { get; set; }

        /// <summary>
        /// 数据行数量
        /// </summary>
        public int RowCount => this.Rows?.Count ?? 0;

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
            this.Context = new DBContext() { DBPath = DBPath };
            this.Id = Guid.NewGuid().ToString();
            this.Rows = GlobalService.GetService<IDataRowCollection>();
            this.Columns = GlobalService.GetService<IDataColumnCollection>();
            this.Rows.Table = this;
            this.Columns.Table = this;
        }

        #region Methods

        /// <summary>
        /// 创建数据表
        /// </summary>
        /// <param name="Name">数据表明</param>
        /// <param name="SqliteTable">对应本地Sqlite 数据表明</param>
        /// <param name="columns">数据列集合</param>
        /// <returns>IDataTable</returns>
        public static async Task<IDataTable> CreateTableAsync(string Name, string SqliteTable, IDataColumnCollection columns)
        {
            var table = GlobalService.GetService<IDataTable>();
            table.Id = Guid.NewGuid().ToString();
            table.Name = Name;
            table.SqliteTable = SqliteTable;
            table.Columns = columns;

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
            var row = GlobalService.GetService<IDataRow>();
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
        public async Task AddColumnsAsync(UpdateSetting setting)
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
        public async Task DelAsync(UpdateSetting setting) => await this.Context.DelAsync(setting);

        /// <summary>
        /// 批量写入数据库
        /// </summary>
        /// <param name="columns">新增的数据列集合</param>
        /// <param name="rows">需要写入数据库的数据行集合</param>
        /// <returns></returns>
        public async Task UpdateAsync(UpdateSetting setting) => await this.Context.UpdateAsync(setting);

        /// <summary>
        /// 合并行
        /// </summary>
        /// <param name="setting">UpdateSetting</param>
        /// <returns>Task</returns>
        /// <exception cref="ArgumentNullException">UpdateSetting.Table, UpdateSetting.Rows</exception>
        public async Task MergeRowsAsync(UpdateSetting setting) => await this.Context.MergeRowsAsync(setting);

        /// <summary>
        /// 合并列
        /// </summary>
        /// <param name="setting">MergeSetting</param>
        /// <returns>Task</returns>
        public async Task MergeColumnsAsync(MergeSetting setting) => await this.Context.MergeColumnsAsync(setting);

        /// <summary>
        /// 合并行
        /// </summary>
        /// <param name="setting">MergeSetting</param>
        /// <returns>Task</returns>
        /// <exception cref="ArgumentNullException">UpdateSetting.Table, UpdateSetting.Rows</exception>
        public async Task MergeRowsAsync(MergeSetting setting) => await this.Context.MergeRowsAsync(setting);

        /// <summary>
        /// 删除数据表
        /// </summary>
        /// <param name="table">需要删除的数据表名</param>
        /// <returns>Task</returns>
        public async Task DropAsync(string table = null) => await this.Context.DropAsync(table ?? this.SqliteTable);

        /// <summary>
        /// 数据表重命名
        /// </summary>
        /// <param name="table">需要删除的数据表名</param>
        /// <returns>Task</returns>
        public async Task RenameAsync(string rename, string originName = null)
        {
            await this.Context.RenameAsync(originName ?? this.SqliteTable, rename);
            this.SqliteTable = rename;
        }

        /// <summary>
        /// 查询数据
        /// </summary>
        /// <param name="setting">QuerySetting</param>
        /// <returns>IDataRowCollection</returns>
        public async Task<IDataRowCollection> QueryAsync(QuerySetting setting) => await this.Context.QueryAsync(setting);

        #endregion Methods
    }
}