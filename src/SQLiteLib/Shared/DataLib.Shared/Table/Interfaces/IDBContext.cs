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
        string DBPath { get; set; }

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
        /// Deletes the asynchronous.
        /// </summary>
        /// <param name="setting">The setting.</param>
        /// <returns></returns>
        Task DelAsync(IUpdateSetting setting);
         
        /// <summary>
        /// Drops the asynchronous.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns></returns>
        Task DropAsync(string table);

        /// <summary>
        /// Executes the non query asynchronous.
        /// </summary>
        /// <param name="sql">The SQL.</param>
        /// <returns></returns>
        Task ExecuteNonQueryAsync(string sql);

        /// <summary>
        /// Inserts the asynchronous.
        /// </summary>
        /// <param name="rows">The rows.</param>
        /// <returns></returns>
        Task<int> InsertAsync(IDataRowCollection rows);

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
        /// Queries the asynchronous.
        /// </summary>
        /// <param name="setting">The setting.</param>
        /// <returns>IDataTable</returns>
        IDataTable Query(IQuerySetting setting);

        /// <summary>
        /// Renames the asynchronous.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <param name="rename">The rename.</param>
        /// <returns></returns>
        Task RenameAsync(string table, string rename);

        /// <summary>
        /// Updates the asynchronous.
        /// </summary>
        /// <param name="setting">The setting.</param>
        /// <returns></returns>
        Task<int> UpdateAsync(IUpdateSetting setting);
    }
}