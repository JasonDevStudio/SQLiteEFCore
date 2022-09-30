using Microsoft.Data.Sqlite;
using SQLiteLib.Table.Impl;
using SQLiteLib.Table.Interfaces;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text;

namespace SQLiteLib
{
    public class DBContext : IDisposable
    {
        #region Properties

        private SqliteConnection connection;

        /// <summary>
        /// 启用日志记录
        /// </summary>
        public static bool UseLogDump = true;

        /// <summary>
        /// 数据库连接字符串
        /// Sqlite: "Data Source=c:\\mydb.db;Version=3;Password=Abc@12345;Cache=Shared;Mode=ReadWriteCreate;Pooling=true;Max Pool Size=1000;"
        /// Sqlite basic : "Data Source=Application.db;Cache=Shared"
        /// </summary>
        public string ConnectionString { get; set; } = "Data Source={0};Version=3;Password={1};Cache=Shared;Mode=ReadWriteCreate;Pooling=true;Max Pool Size=1000;";

        /// <summary>
        /// DB Path
        /// </summary>
        public string DBPath { get; set; }

        /// <summary>
        /// Gets or sets the caching mode used by the connection.
        /// </summary>
        public SqliteCacheMode Cache { get; set; } = SqliteCacheMode.Shared;

        /// <summary>
        /// Gets or sets the connection mode
        /// </summary>
        public SqliteOpenMode Mode { get; set; } = SqliteOpenMode.ReadWriteCreate;

        /// <summary>
        /// Password
        /// </summary>
        public string Password { get; set; }

        /// <summary>
        /// Gets or sets the default Microsoft.Data.Sqlite.SqliteConnection.DefaultTimeout value.
        /// </summary>
        public int DefaultTimeout { get; set; } = 30;

        /// <summary>
        /// Gets or sets a value indicating whether the connection will be pooled.
        /// </summary>
        public bool Pooling { get; set; } = true;

        #endregion

        #region OnConfiguring

        /// <summary>
        /// DBContext
        /// </summary>
        public DBContext()
        {
            this.OnConfiguring();
        }

        /// <summary>
        /// Called when [configuring].
        /// </summary> 
        protected async Task OnConfiguring()
        {
            if (string.IsNullOrWhiteSpace(this.ConnectionString))
            {
                var builder = new SqliteConnectionStringBuilder($"Data Source={this.DBPath};Cache=Shared")
                {
                    Mode = this.Mode,
                    Password = this.Password,
                    DefaultTimeout = this.DefaultTimeout,
                    Pooling = this.Pooling,
                    Cache = this.Cache,
                };

                this.ConnectionString = builder.ToString();
            }
            else
            {
                this.ConnectionString = String.Format(this.ConnectionString, this.DBPath, this.Password);
            }

            this.connection = new SqliteConnection(this.ConnectionString);

            if (this.connection.State != System.Data.ConnectionState.Open)
                await this.connection.OpenAsync();
        }

        /// <summary>
        /// Dispose
        /// </summary> 
        public void Dispose()
        {
            this.connection?.Close();
            this.connection?.Dispose();
        }

        #endregion

        #region Query

        /// <summary>
        /// 查询数据
        /// </summary>
        /// <param name="table">IDataTable</param>
        /// <param name="columns">需要查询的数据列集合</param>
        /// <returns>IDataRowCollection</returns>
        public IDataRowCollection Query(IDataTable table, DataColumnCollection columns, params SqliteParameter[] parameters)
        {
            var rows = new DataRowCollection() { Table = table };
            var sql = new StringBuilder();
            var cmd = new SqliteCommand();


            return rows;
        }

        #endregion

        #region BulkInsert

        /// <summary>
        /// 批量写入数据库
        /// </summary>
        /// <typeparam name="T">数据库实体类型</typeparam>
        /// <param name="entites">需要写入数据库的实体集合</param>
        /// <param name="options">BulkOperation</param>
        /// <returns>dynamic</returns>
        public void BulkInserts<T>(IEnumerable<T> entites, Action<BulkOperation<T>> options = null)
            where T : class, new()
        {
            var sqlLog = new StringBuilder();
            options ??= GetOptions(sqlLog);
            this.BulkInsert(entites, options);
        }

        #endregion
    }
}