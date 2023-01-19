using DataLib.Table.Interfaces;

namespace DataLib.Table.Impl;

public abstract class DBContextBasic : IDBContext
{
    #region Properties
     
    /// <summary>
    /// DB Path
    /// </summary>
    public string DBPath { get; set; } 

    #endregion Properties

    #region OnConfiguring

    /// <summary>
    /// DBContext
    /// </summary>
    public DBContextBasic()
    {
        this.OnConfiguring();
    }

    /// <summary>
    /// Called when [configuring].
    /// </summary>
    protected abstract Task OnConfiguring();

    /// <summary>
    /// Dispose
    /// </summary>
    public abstract void Dispose();

    #endregion OnConfiguring

    #region Query

    /// <summary>
    /// 查询数据
    /// </summary>
    /// <param name="setting">数据查询参数设置</param>
    /// <returns>IDataRowCollection</returns>
    public abstract Task<IDataRowCollection> QueryAsync(IQuerySetting setting);

    #endregion Query

    #region Insert Del Update Del Rename Drop

    /// <summary>
    /// 创建数据表
    /// </summary>
    /// <param name="table">IDataTable</param>
    /// <returns>Task</returns>
    public abstract Task CreateTableAsync(IDataTable table);

    /// <summary>
    /// 批量写入数据库
    /// </summary>
    /// <param name="rows">需要写入数据库的数据行集合</param>
    /// <returns>Task</returns>
    public abstract Task<int> InsertAsync(IDataRowCollection rows);

    /// <summary>
    /// 新增数据列
    /// </summary>
    /// <param name="setting">UpdateSetting</param>
    /// <returns>Task</returns>
    public abstract Task AddColumnsAsync(IUpdateSetting setting);

    /// <summary>
    /// 删除数据
    /// </summary>
    /// <param name="setting">UpdateSetting</param>
    /// <returns>Task</returns>
    public abstract Task DelAsync(IUpdateSetting setting);

    /// <summary>
    /// 批量更新数据库
    /// </summary>
    /// <param name="columns">新增的数据列集合</param>
    /// <param name="rows">需要写入数据库的数据行集合</param>
    /// <returns></returns>
    public abstract Task<int> UpdateAsync(IUpdateSetting setting);

    /// <summary>
    /// 合并行
    /// </summary>
    /// <param name="setting">UpdateSetting</param>
    /// <returns>Task</returns>
    /// <exception cref="ArgumentNullException">UpdateSetting.Table, UpdateSetting.Rows</exception>
    public abstract Task MergeRowsAsync(IUpdateSetting setting);

    /// <summary>
    /// 合并行
    /// </summary>
    /// <param name="setting">MergeSetting</param>
    /// <returns>Task</returns>
    /// <exception cref="ArgumentNullException">UpdateSetting.Table, UpdateSetting.Rows</exception>
    public abstract Task MergeRowsAsync(IMergeSetting setting);

    /// <summary>
    /// 合并列
    /// </summary>
    /// <param name="setting">MergeSetting</param>
    /// <returns>Task</returns>
    public abstract Task MergeColumnsAsync(IMergeSetting setting);

    /// <summary>
    /// 删除数据表
    /// </summary>
    /// <param name="table">需要删除的数据表名</param>
    /// <returns>Task</returns>
    public abstract Task DropAsync(string table);

    /// <summary>
    /// 数据表重命名
    /// </summary>
    /// <param name="table">需要删除的数据表名</param>
    /// <returns>Task</returns>
    public abstract Task RenameAsync(string table, string rename);

    /// <summary>
    /// Executes the non query asynchronous.
    /// </summary>
    /// <param name="sql">The SQL.</param>
    /// <exception cref="ArgumentNullException">sql</exception>
    public virtual Task ExecuteNonQueryAsync(string sql)
    { 
        throw new NotImplementedException();
    }

    #endregion Insert Del Update Del Rename Drop
     
}