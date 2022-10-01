using Microsoft.Data.Sqlite;
using SQLiteEFCore.Shared.DB;
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
        public async Task<IDataRowCollection> QueryAsync(IDataTable table, DataColumnCollection columns, List<Condition> parameters, DataColumnCollection orderFields = null)
        {
            await this.OnConfiguring();
            var rows = new DataRowCollection() { Table = table };
            var sql = new StringBuilder();
            var cmd = new SqliteCommand();
            var whereSql = Condition.BuildSql(parameters);
            var orderSql = Condition.BuildOrderSql(orderFields);
            sql.Append($"SELECT {string.Join(',', columns.Select(c => c.Field))} FROM {table.Name} {whereSql} ");
            sql.Append(whereSql);
            sql.Append(orderSql);
            cmd.CommandText = sql.ToString();
            cmd.Connection = this.connection;
            var reader = cmd.ExecuteReader();

            while (await reader.ReadAsync())
            {
                var row = table.NewRow();
                for (int i = 0; i < columns.Count; i++)
                {
                    var column = columns[i];
                    row[column.ColumnIndex] = reader[i];
                }

                rows.Add(row);
            }

            return rows;
        }

        #endregion

        #region BulkInsert

        /// <summary>
        /// 批量写入数据库
        /// </summary> 
        /// <param name="rows">需要写入数据库的数据行集合</param>
        /// <returns>Task</returns>
        public async Task WriteAsync(DataRowCollection rows)
        {
            await this.OnConfiguring();
            using var tran = await this.connection.BeginTransactionAsync();

            try
            {
                var columns = rows.Table.Columns;
                var fieldStr = string.Join(',', columns.Select(c => c.Field));
                var paraStr = string.Join(',', columns.Select(c => $"${c.Field}"));
                var sql = $"INSERT INTO {rows.Table.Name} ({fieldStr}) VALUES({paraStr})";
                var cmd = new SqliteCommand(sql, this.connection);
                var recount = 0;

                foreach (var row in rows)
                {
                    cmd.Parameters?.Clear();

                    foreach (var col in columns)
                        cmd.Parameters.AddWithValue($"${col.Field}", row[col]);

                    recount += await cmd.ExecuteNonQueryAsync();
                }

                await tran.CommitAsync();
            }
            catch (Exception ex)
            {
                await tran.RollbackAsync();
                throw;
            }
        }

        /// <summary>
        /// 新增数据列
        /// </summary>
        /// <param name="setting">UpdateSetting</param>
        /// <returns>Task</returns>
        public async Task AddColumnsAsync(UpdateSetting setting)
        {
            await this.OnConfiguring();

            try
            {
                var cmd = new SqliteCommand() { Connection = this.connection };
                var recount = 0;

                foreach (var col in setting.AddColumns)
                {
                    cmd.CommandText = $"ALTER TABLE {setting.Table.Name} ADD COLUMN {col.Field} {Enum.GetName(GetSqliteType(col.TypeCode))} ";
                    recount += await cmd.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary>
        /// 批量写入数据库
        /// </summary>
        /// <param name="columns">新增的数据列集合</param>
        /// <param name="rows">需要写入数据库的数据行集合</param>
        /// <returns></returns>
        public async Task UpdateAsync(UpdateSetting setting)
        {
            await this.OnConfiguring();
            using var tran = await this.connection.BeginTransactionAsync();

            try
            {
                var sqlBuilder = new StringBuilder();
                var cmd = new SqliteCommand() { Connection = this.connection };
                var recount = 0;

                sqlBuilder.Append($"UPDATE {setting.Table.Name} SET ");

                for (int i = 0; i < setting.UpdateColumns.Count; i++)
                {
                    if (i > 0)
                        sqlBuilder.Append($",");

                    var col = setting.UpdateColumns[i];
                    sqlBuilder.Append($"{col.Field} = ${col.Field}");
                }

                sqlBuilder.Append($" WHERE ");

                for (int i = 0; i < setting.PrimaryColumns.Count; i++)
                {
                    if (i > 0)
                        sqlBuilder.Append($" {QueryLogic.AND} ");

                    var col = setting.PrimaryColumns[i];
                    sqlBuilder.Append($"{col.Field} = ${col.Field}");
                }

                cmd.CommandText = sqlBuilder.ToString();

                foreach (var row in setting.Rows)
                {
                    cmd.Parameters?.Clear();

                    foreach (var col in setting.UpdateColumns)
                        cmd.Parameters.AddWithValue($"${col.Field}", row[col]);

                    foreach (var col in setting.PrimaryColumns)
                        cmd.Parameters.AddWithValue($"${col.Field}", row[col]);

                    recount += await cmd.ExecuteNonQueryAsync();
                }

                await tran.CommitAsync();
            }
            catch (Exception ex)
            {
                await tran.RollbackAsync();
                throw;
            }
        }

        #endregion

        #region private method

        /// <summary>
        /// 通过C# 类型Code 获取Sqlite Type
        /// </summary>
        /// <param name="code">C# TypeCode</param>
        /// <returns>SqliteType</returns>
        private SqliteType GetSqliteType(TypeCode code)
        {
            switch (code)
            {
                case TypeCode.Object:
                    return SqliteType.Blob;
                case TypeCode.Boolean:
                case TypeCode.SByte:
                case TypeCode.Byte:
                case TypeCode.Int16:
                case TypeCode.UInt16:
                case TypeCode.Int32:
                case TypeCode.UInt32:
                case TypeCode.Int64:
                case TypeCode.UInt64:
                    return SqliteType.Integer;
                case TypeCode.Single:
                case TypeCode.Double:
                case TypeCode.Decimal:
                    return SqliteType.Real;
                case TypeCode.DateTime:
                case TypeCode.String:
                case TypeCode.Char:
                case TypeCode.DBNull:
                case TypeCode.Empty:
                default:
                    return SqliteType.Text;
            }
        }

        #endregion
    }
}