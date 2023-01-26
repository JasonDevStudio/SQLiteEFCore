namespace DataLib.Table.Interfaces
{
    /// <summary>
    /// IDataTable
    /// </summary>
    public interface IDataTable
    {
        /// <summary>
        /// 数据上下文
        /// </summary>
        [Newtonsoft.Json.JsonIgnore]
        IDBContext Context { get; set; }

        /// <summary>
        /// Id
        /// </summary>
        string Id { get; set; }

        /// <summary>
        /// Name
        /// </summary>
        string Name { get; set; }

        /// <summary>
        /// Original Table Name
        /// </summary>
        string OriginalTable { get; set; }

        /// <summary>
        /// 数据行数量
        /// </summary>
        int RowCount { get; }

        /// <summary>
        /// 数据列数量
        /// </summary>
        int ColumnCount { get; }

        /// <summary>
        /// Gets or sets the database file.
        /// </summary>
        /// <value>
        /// The database file.
        /// </value>
        string DBFile { get; set; }

        /// <summary>
        /// Gets or sets the mode.
        /// </summary>
        /// <value>
        /// The mode.
        /// </value>
        TableMode Mode { get; set; }

        /// <summary>
        /// 数据列集合
        /// </summary>
        IDataColumnCollection Columns { get; set; }

        /// <summary>
        /// 数据行集合
        /// </summary>
        IDataRowCollection Rows { get; set; }

        /// <summary>
        /// 获取数据行
        /// </summary>
        /// <param name="index">数据行索引</param>
        /// <returns>IDataRow</returns>
        IDataRow this[int index] { get; }

        /// <summary>
        /// 创建新数据行
        /// </summary>
        /// <returns>IDataRow</returns>
        IDataRow NewRow();

        /// <summary>
        /// 刷新图表
        /// </summary>
        Task ReflashAsync();

        /// <summary>
        /// 批量写入数据库
        /// </summary>
        /// <returns>Task</returns>
        Task WriteAsync();

        /// <summary>
        /// 批量写入数据库
        /// </summary>
        /// <param name="rows">需要写入数据库的数据行集合</param>
        /// <returns>Task</returns>
        Task InsertAsync(IDataRowCollection rows);

        /// <summary>
        /// 新增数据列
        /// </summary>
        /// <param name="setting">UpdateSetting</param>
        /// <returns>Task</returns>
        Task AddColumnsAsync(IUpdateSetting setting);

        /// <summary>
        /// 删除数据
        /// </summary>
        /// <param name="setting">UpdateSetting</param>
        /// <returns>Task</returns>
        Task DelAsync(IUpdateSetting setting);

        /// <summary>
        /// 批量写入数据库
        /// </summary>
        /// <param name="columns">新增的数据列集合</param>
        /// <param name="rows">需要写入数据库的数据行集合</param>
        /// <returns></returns>
        Task UpdateAsync(IUpdateSetting setting);

        /// <summary>
        /// 合并行
        /// </summary>
        /// <param name="setting">UpdateSetting</param>
        /// <returns>Task</returns>
        /// <exception cref="ArgumentNullException">UpdateSetting.Table, UpdateSetting.Rows</exception>
        Task MergeRowsAsync(IUpdateSetting setting);

        /// <summary>
        /// 合并行
        /// </summary>
        /// <param name="setting">MergeSetting</param>
        /// <returns>Task</returns>
        /// <exception cref="ArgumentNullException">UpdateSetting.Table, UpdateSetting.Rows</exception>
        Task MergeRowsAsync(IMergeSetting setting);

        /// <summary>
        /// 合并列
        /// </summary>
        /// <param name="setting">MergeSetting</param>
        /// <returns>Task</returns>
        Task MergeColumnsAsync(IMergeSetting setting);

        /// <summary>
        /// 删除数据表
        /// </summary>
        /// <param name="table">需要删除的数据表名</param>
        /// <returns>Task</returns>
        Task DropAsync(string table = null);

        /// <summary>
        /// 数据表重命名
        /// </summary>
        /// <param name="table">需要删除的数据表名</param>
        /// <returns>Task</returns>
        Task RenameAsync(string table, string rename);

        /// <summary>
        /// 查询数据
        /// </summary>
        /// <param name="setting">QuerySetting</param>
        /// <returns>IDataRowCollection</returns>
        Task<IDataTable> QueryAsync(IQuerySetting setting);

        /// <summary>
        /// Executes the non query asynchronous.
        /// </summary>
        /// <param name="sql">The SQL.</param>
        /// <exception cref="ArgumentNullException">sql</exception>
        Task ExecuteNonQueryAsync(string sql);
    }
}