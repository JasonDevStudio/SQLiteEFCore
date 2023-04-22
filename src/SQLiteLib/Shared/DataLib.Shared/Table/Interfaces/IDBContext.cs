using DataLib.Table;
using DataLib.Table.Interfaces;

namespace DataLib.Table
{
    /// <summary>
    /// IDBContext
    /// </summary>
    public interface IDBContext : IDisposable
    {
        /// <summary>
        /// Gets or sets the database path.
        /// </summary>
        /// <value>
        /// The database path.
        /// </value>
        string DBFile { get; init; }

        /// <summary>
        /// Gets or sets the data table.
        /// </summary>
        /// <value>
        /// The data table.
        /// </value>
        [Newtonsoft.Json.JsonIgnore]
        [System.Text.Json.Serialization.JsonIgnore]
        IDataTable DataTable { get; }

        /// <summary>
        /// Adds the columns asynchronous.
        /// </summary>
        /// <param name="setting">The setting.</param>
        /// <returns></returns>
        Task AddColumnsAsync(IUpdateSetting setting);

        /// <summary>
        /// Creates the table asynchronous.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns></returns>
        Task CreateTableAsync(IDataTable table);
 
        /// <summary>
        /// Executes the non query asynchronous.
        /// </summary>
        /// <param name="sql">The SQL.</param>
        /// <returns></returns>
        Task ExecuteNonQueryAsync(string sql);

       /// <summary>
       /// 写入数据
       /// </summary>
       /// <returns>Task</returns>
        Task WriteAsync();
        
        /// <summary>
        /// Inserts the asynchronous.
        /// </summary>
        /// <param name="rows">The rows.</param>
        /// <returns></returns>
        Task WriteAsync(IDataRowCollection rows);

        /// <summary>
        /// Merges the columns asynchronous.
        /// </summary>
        /// <param name="setting">The setting.</param>
        /// <returns></returns>
        Task MergeColumnsAsync(IMergeSetting setting);

        /// <summary>
        /// Merges the rows asynchronous.
        /// </summary>
        /// <param name="setting">The setting.</param>
        /// <returns></returns>
        Task MergeRowsAsync(IMergeSetting setting);

        /// <summary>
        /// Merges the rows asynchronous.
        /// </summary>
        /// <param name="setting">The setting.</param>
        /// <returns></returns>
        Task MergeRowsAsync(IUpdateSetting setting);

        /// <summary>
        /// Queries the asynchronous.
        /// </summary>
        /// <param name="setting">The setting.</param>
        /// <returns>IDataTable</returns>
        Task<IDataTable> QueryAsync(IQuerySetting setting);

        /// <summary>
        /// Queries the row count.
        /// </summary>
        /// <param name="setting">The setting.</param>
        /// <returns></returns>
        Task<int> QueryRowCountAsync();

        /// <summary>
        /// Updates the asynchronous.
        /// </summary>
        /// <param name="setting">The setting.</param>
        /// <returns></returns>
        Task<int> UpdateAsync(IUpdateSetting setting);
    }
}