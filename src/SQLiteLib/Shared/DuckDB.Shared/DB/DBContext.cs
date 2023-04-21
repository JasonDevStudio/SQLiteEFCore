using System.Data;
using System.Text;
using DataLib;
using DataLib.Table;
using DataLib.Table.Impl;
using DataLib.Table.Interfaces;
using DuckDB.NET;
using DuckDB.NET.Data;

namespace DuckDB.Lib
{
    public class DBContext : IDisposable
    {
        #region Properties

        private DuckDBConnection connection;

        /// <summary>
        /// 启用日志记录
        /// </summary>
        public static bool UseLogDump = true;

        /// <summary>
        /// 连接字符串格式化
        /// </summary>
        public const string ConnectionStringFormat = "Data Source={0};Password={1};Cache=Shared;Mode=ReadWriteCreate;Pooling=true;";

        /// <summary>
        /// 数据库连接字符串
        /// Sqlite: "Data Source=c:\\mydb.db;Version=3;Password=Abc@12345;Cache=Shared;Mode=ReadWriteCreate;Pooling=true;Max Pool Size=1000;"
        /// Sqlite basic : "Data Source=Application.db;Cache=Shared"
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// DB Path
        /// </summary>
        public string DBFile { get; set; } = ":memory:";

        #endregion Properties

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
            this.ConnectionString = $"Data Source={this.DBFile}";

            this.connection = new DuckDBConnection(this.ConnectionString);

            if (this.connection.State != ConnectionState.Open)
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

        #endregion OnConfiguring

        #region Query

        /// <summary>
        /// 查询数据
        /// </summary>
        /// <param name="setting">数据查询参数设置</param>
        /// <returns>IDataRowCollection</returns>
        public async Task<IDataRowCollection> QueryAsync(QuerySetting setting)
        {
            if (setting.Table == null)
                throw new ArgumentNullException(nameof(QuerySetting.Table));

            if (!(setting.Columns?.Any() ?? false))
                throw new ArgumentNullException(nameof(QuerySetting.Columns));

            await this.OnConfiguring();
            var rows = GlobalService.GetService<IDataRowCollection>();
            var cmd = new DuckDbCommand();
            var whereSql = DuckDBQueryFilter.BuildWhereSql(setting.Parameters);
            var orderSql = DuckDBQueryFilter.BuildOrderSql(setting.OrderFields);
            var sql = $"SELECT {string.Join(',', setting.Columns.Select(c => c.Field))} FROM {setting.Table.OriginalTable} {whereSql} {orderSql}";
            cmd.CommandText = sql.ToString();
            cmd.Connection = this.connection;
            var reader = cmd.ExecuteReader();

            while (await reader.ReadAsync())
            {
                var row = setting.Table.NewRow();
                for (int i = 0; i < setting.Columns.Count; i++)
                {
                    var column = setting.Columns[i];
                    row[column.ColumnIndex] = reader[i];
                }

                rows.Add(row);
            }

            Console.WriteLine(sql);

            rows.Table = setting.Table;
            return rows;
        }

        #endregion Query

        #region Insert Del Update Del Rename Drop

        /// <summary>
        /// 创建数据表
        /// </summary>
        /// <param name="table">IDataTable</param>
        /// <returns>Task</returns>
        public async Task CreateTableAsync(IDataTable table)
        {
            if (table == null)
                throw new ArgumentNullException(nameof(table));

            if (string.IsNullOrWhiteSpace(table.OriginalTable))
                throw new ArgumentNullException(nameof(table.OriginalTable));

            if (!(table.Columns?.Any() ?? false))
                throw new ArgumentNullException(nameof(table.Columns));

            await this.OnConfiguring();
            var sqlBuilder = new StringBuilder();
            var cmd = new DuckDbCommand() { Connection = this.connection };
            var recount = 0;

            sqlBuilder.Append($"CREATE TABLE {table.OriginalTable} (");

            for (int i = 0; i < table.ColumnCount; i++)
            {
                if (i > 0)
                    sqlBuilder.Append(",");

                var col = table.Columns[i];
                sqlBuilder.Append($"{col.Field} {GetSqliteType(col.TypeCode)} {(col.IsPK ? "PRIMARY KEY" : string.Empty)} ");
            }

            sqlBuilder.Append(")");

            cmd.CommandText = sqlBuilder.ToString();
            recount += await cmd.ExecuteNonQueryAsync();
        }

        /// <summary>
        /// 批量写入数据库
        /// </summary>
        /// <param name="rows">需要写入数据库的数据行集合</param>
        /// <returns>Task</returns>
        public async Task<int> InsertAsync(IDataRowCollection rows)
        {
            if (!(rows?.Any() ?? false))
                throw new ArgumentNullException(nameof(rows));

            if (rows.Table == null)
                throw new ArgumentNullException(nameof(rows.Table));

            if (string.IsNullOrWhiteSpace(rows.Table.OriginalTable))
                throw new ArgumentNullException(nameof(rows.Table.OriginalTable));

            var tran = this.connection.BeginTransaction();

            try
            {
                await this.OnConfiguring();
                var pragmaCmd = new DuckDbCommand("PRAGMA journal_mode = WAL;", this.connection);
                await pragmaCmd.ExecuteNonQueryAsync();
                pragmaCmd = new DuckDbCommand("PRAGMA synchronous = OFF;", this.connection);
                await pragmaCmd.ExecuteNonQueryAsync();

                var columns = rows.Table.Columns;
                var fieldStr = string.Join(',', columns.Where(c => !c.IsAutoincrement).Select(c => c.Field));
                var paraStr = string.Join(',', columns.Where(c => !c.IsAutoincrement).Select(c => $"${c.Field}"));
                var sql = $"INSERT INTO {rows.Table.OriginalTable} ({fieldStr}) VALUES({paraStr})";
                var cmd = new DuckDbCommand(sql, this.connection);
                var recount = 0;

                for (int i = 0; i < rows.Count; i++)
                {
                    var row = rows[i];
                    cmd.Parameters?.Clear();
                    columns.ForEach(col => cmd.Parameters.Add(new DuckDBParameter($"${col.Field}", row[col])));
                    recount += await cmd.ExecuteNonQueryAsync();
                }

                await tran?.CommitAsync();
                return recount;
            }
            catch (Exception e)
            {
                tran?.Rollback();
                Console.WriteLine(e);
                throw;
            }
            finally
            {
                tran?.Dispose();
            }
        }

        /// <summary>
        /// 新增数据列
        /// </summary>
        /// <param name="setting">UpdateSetting</param>
        /// <returns>Task</returns>
        public async Task AddColumnsAsync(UpdateSetting setting)
        {
            if (string.IsNullOrWhiteSpace(setting.Table))
                throw new ArgumentNullException(nameof(UpdateSetting.Table));

            if (!(setting.NewColumns?.Any() ?? false))
                throw new ArgumentNullException(nameof(UpdateSetting.NewColumns));

            await this.OnConfiguring();
            var cmd = new DuckDbCommand() { Connection = this.connection };
            var recount = 0;

            for (int i = 0; i < setting.NewColumns.Count; i++)
            {
                var col = setting.NewColumns[i];
                cmd.CommandText = $"ALTER TABLE {setting.Table} ADD COLUMN {col.Field} {Enum.GetName(GetSqliteType(col.TypeCode))} ";
                recount += await cmd.ExecuteNonQueryAsync();
            }
        }

        /// <summary>
        /// 删除数据
        /// </summary>
        /// <param name="setting">UpdateSetting</param>
        /// <returns>Task</returns>
        public async Task DelAsync(UpdateSetting setting)
        {
            if (string.IsNullOrWhiteSpace(setting.Table))
                throw new ArgumentNullException(nameof(UpdateSetting.Table));

            if (setting.Parameters == null || !setting.Parameters.Any())
                throw new ArgumentNullException(nameof(UpdateSetting.Parameters));

            await this.OnConfiguring();
            var sqlBuilder = new StringBuilder();
            var cmd = new DuckDbCommand() { Connection = this.connection };
            var recount = 0;

            sqlBuilder.Append($"DELTE FROM {setting.Table} ");
            sqlBuilder.Append(DuckDBQueryFilter.BuildWhereSql(setting.Parameters));

            cmd.CommandText = sqlBuilder.ToString();
            recount += await cmd.ExecuteNonQueryAsync();
        }

        /// <summary>
        /// 批量更新数据库
        /// </summary>
        /// <param name="columns">新增的数据列集合</param>
        /// <param name="rows">需要写入数据库的数据行集合</param>
        /// <returns></returns>
        public async Task<int> UpdateAsync(UpdateSetting setting)
        {
            if (string.IsNullOrWhiteSpace(setting.Table))
                throw new ArgumentNullException(nameof(UpdateSetting.Table));

            if (setting.UpdateColumns == null || !setting.UpdateColumns.Any())
                throw new ArgumentNullException(nameof(UpdateSetting.UpdateColumns));

            if (setting.PrimaryColumns == null || !setting.PrimaryColumns.Any())
                throw new ArgumentNullException(nameof(UpdateSetting.PrimaryColumns));

            if (setting.NewColumns?.Any() ?? false)
                await this.AddColumnsAsync(setting);

            await this.OnConfiguring();
            using var tran = this.connection.BeginTransaction();
            var sqlBuilder = new StringBuilder();
            var cmd = new DuckDbCommand() { Connection = this.connection, Transaction = tran };
            var columns = setting.UpdateColumns.Where(c => !c.IsAutoincrement).ToList();
            var recount = 0;

            sqlBuilder.Append($"UPDATE {setting.Table} SET ");

            for (int i = 0; i < columns.Count; i++)
            {
                if (i > 0)
                    sqlBuilder.Append($",");

                var col = columns[i];
                sqlBuilder.Append($"{col.Field} = ${col.Field}");
            }

            sqlBuilder.Append($" WHERE ");

            for (int i = 0; i < setting.PrimaryColumns.Count; i++)
            {
                if (i > 0)
                    sqlBuilder.Append($" {LogicMode.AND} ");

                var col = setting.PrimaryColumns[i];
                sqlBuilder.Append($"{col.Field} = ${col.Field}");
            }

            cmd.CommandText = sqlBuilder.ToString();

            for (int i = 0; i < setting.Rows.Count; i++)
            {
                cmd.Parameters?.Clear();
                var row = setting.Rows[i];
                columns.ForEach(col => cmd.Parameters.Add(new DuckDBParameter($"${col.Field}", row[col])));
                setting.PrimaryColumns.ForEach(col => cmd.Parameters.Add(new DuckDBParameter($"${col.Field}", row[col])));
                recount += await cmd.ExecuteNonQueryAsync();
            }

            await tran.CommitAsync();
            return recount;
        }

        /// <summary>
        /// 合并行
        /// </summary>
        /// <param name="setting">UpdateSetting</param>
        /// <returns>Task</returns>
        /// <exception cref="ArgumentNullException">UpdateSetting.Table, UpdateSetting.Rows</exception>
        public async Task MergeRowsAsync(UpdateSetting setting)
        {
            if (string.IsNullOrWhiteSpace(setting.Table))
                throw new ArgumentNullException(nameof(UpdateSetting.Table));

            if (!(setting.Rows?.Any() ?? false))
                throw new ArgumentNullException(nameof(UpdateSetting.Rows));

            await this.OnConfiguring();

            if (setting.NewColumns?.Any() ?? false)
                await this.AddColumnsAsync(setting);

            await this.InsertAsync(setting.Rows);
        }

        /// <summary>
        /// 合并行
        /// </summary>
        /// <param name="setting">MergeSetting</param>
        /// <returns>Task</returns>
        /// <exception cref="ArgumentNullException">UpdateSetting.Table, UpdateSetting.Rows</exception>
        public async Task MergeRowsAsync(MergeSetting setting)
        {
            if (string.IsNullOrWhiteSpace(setting.TableName))
                throw new ArgumentNullException(nameof(MergeSetting.RightTableName));

            if (string.IsNullOrWhiteSpace(setting.TableName))
                throw new ArgumentNullException(nameof(MergeSetting.RightTableName));

            if (!(setting.LeftColumns?.Any() ?? false))
                throw new ArgumentNullException(nameof(MergeSetting.LeftColumns));

            if (!(setting.RightColumns?.Any() ?? false))
                throw new ArgumentNullException(nameof(MergeSetting.RightColumns));

            if (!(setting.MacthCloumns?.Any() ?? false))
                throw new ArgumentNullException(nameof(MergeSetting.MacthCloumns));

            await this.OnConfiguring();
            var columns = setting.LeftColumns.Copy();

            if (setting.NewColumns?.Any() ?? false)
            {
                columns.AddRange(setting.NewColumns.Copy());
                await this.AddColumnsAsync(new UpdateSetting { Table = setting.TableName, NewColumns = setting.NewColumns });
            }

            var fieldStr = string.Join(',', columns.Where(c => !c.IsAutoincrement).Select(c => c.Field));
            var rfieldStr = string.Join(',', setting.RightColumns.Where(c => !c.IsAutoincrement).Select(c => c.Field));
            var paraStr = string.Join(',', columns.Select(c => $"${c.Field}"));
            var sql = $"INSERT INTO {setting.TableName} ({fieldStr}) SELECT {rfieldStr} FROM {setting.RightTableName}";
            var cmd = new DuckDbCommand(sql, this.connection);
            var recount = await cmd.ExecuteNonQueryAsync();
        }

        /// <summary>
        /// 合并列
        /// </summary>
        /// <param name="setting">MergeSetting</param>
        /// <returns>Task</returns>
        public async Task MergeColumnsAsync(MergeSetting setting)
        {
            if (string.IsNullOrWhiteSpace(setting.TableName))
                throw new ArgumentNullException(nameof(MergeSetting.RightTableName));

            if (string.IsNullOrWhiteSpace(setting.TableName))
                throw new ArgumentNullException(nameof(MergeSetting.RightTableName));

            if (!(setting.LeftColumns?.Any() ?? false))
                throw new ArgumentNullException(nameof(MergeSetting.LeftColumns));

            if (!(setting.RightColumns?.Any() ?? false))
                throw new ArgumentNullException(nameof(MergeSetting.RightColumns));

            if (!(setting.MacthCloumns?.Any() ?? false))
                throw new ArgumentNullException(nameof(MergeSetting.MacthCloumns));

            await this.OnConfiguring();

            #region Rename

            var rename = $"{setting.TableName}_{DateTime.Now:MMddHHmmss}";
            await this.RenameAsync(setting.TableName, rename);
            setting.TableName = rename;

            #endregion Rename

            #region Build Sql

            var sqlBuilder = new StringBuilder();
            var leftFields = setting.LeftColumns?.Select(f => $"L.{f.Field}");
            var rightFields = setting.RightColumns?.Select(f => $"R.{f.Field}");
            sqlBuilder.Append($"CREATE TABLE {setting.TableName} AS SELECT ");

            if (leftFields?.Any() ?? false)
                sqlBuilder.Append($"{string.Join(',', leftFields)},");

            if (rightFields?.Any() ?? false)
                sqlBuilder.Append(string.Join(',', rightFields));

            sqlBuilder.Append($" FROM {setting.TableName} L ");

            switch (setting.Join)
            {
                case JoinMode.RIGHT_JOIN:
                    sqlBuilder.Append($"RIGHT JOIN ");
                    break;

                case JoinMode.LEFT_JOIN:
                    sqlBuilder.Append($"LEFT JOIN ");
                    break;

                case JoinMode.INNER_JOIN:
                default:
                    sqlBuilder.Append($"INNER JOIN ");
                    break;
            }

            sqlBuilder.Append($"{setting.RightTableName} R ON ");

            for (int i = 0; i < setting.MacthCloumns.Count; i++)
            {
                var exp = setting.MacthCloumns[i];
                sqlBuilder.Append($" L.{exp.Left.Field} =  R.{exp.right.Field} ");
            }

            #endregion Build Sql

            var cmd = new DuckDbCommand() { Connection = this.connection, CommandText = sqlBuilder.ToString() };
            await cmd.ExecuteNonQueryAsync();
        }

        /// <summary>
        /// 删除数据表
        /// </summary>
        /// <param name="table">需要删除的数据表名</param>
        /// <returns>Task</returns>
        public async Task DropAsync(string table)
        {
            if (string.IsNullOrWhiteSpace(table))
                throw new ArgumentNullException(nameof(table));

            await this.OnConfiguring();
            var cmd = new DuckDbCommand() { Connection = this.connection };

            cmd.CommandText = $"DROP TABLE {table} ";
            await cmd.ExecuteNonQueryAsync();
        }

        /// <summary>
        /// 数据表重命名
        /// </summary>
        /// <param name="table">需要删除的数据表名</param>
        /// <returns>Task</returns>
        public async Task RenameAsync(string table, string rename)
        {
            if (string.IsNullOrWhiteSpace(table))
                throw new ArgumentNullException(nameof(table));

            await this.OnConfiguring();
            var cmd = new DuckDbCommand() { Connection = this.connection };

            cmd.CommandText = $"ALTER TABLE {table} RENAME TO {rename}";
            await cmd.ExecuteNonQueryAsync();
        }

        /// <summary>
        /// Executes the non query asynchronous.
        /// </summary>
        /// <param name="sql">The SQL.</param>
        /// <exception cref="ArgumentNullException">sql</exception>
        public async Task ExecuteNonQueryAsync(string sql)
        {
            if (string.IsNullOrWhiteSpace(sql))
                throw new ArgumentNullException(nameof(sql));

            await this.OnConfiguring();
            var cmd = new DuckDbCommand() { Connection = this.connection };

            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync();
        }

        #endregion Insert Del Update Del Rename Drop

        #region private method

        /// <summary>
        /// 通过C# 类型Code 获取Sqlite Type
        /// </summary>
        /// <param name="code">C# TypeCode</param>
        /// <returns>SqliteType</returns>
        private DuckDBType GetSqliteType(TypeCode code)
        {
            switch (code)
            {
                case TypeCode.Object:
                    return DuckDBType.DuckdbTypeBlob;
                case TypeCode.Boolean:
                    return DuckDBType.DuckdbTypeBoolean;
                case TypeCode.SByte:
                case TypeCode.Byte:
                case TypeCode.Int16:
                case TypeCode.UInt16:
                case TypeCode.Int32:
                case TypeCode.UInt32:
                    return DuckDBType.DuckdbTypeInteger;
                case TypeCode.Int64:
                case TypeCode.UInt64:
                    return DuckDBType.DuckdbTypeBigInt;
                case TypeCode.Single:
                case TypeCode.Double:
                case TypeCode.Decimal:
                    return DuckDBType.DuckdbTypeDouble;
                case TypeCode.DateTime:
                    return DuckDBType.DuckdbTypeTimestamp;
                case TypeCode.String:
                case TypeCode.Char:
                    return DuckDBType.DuckdbTypeVarchar;
                case TypeCode.DBNull:
                case TypeCode.Empty:
                default:
                    return DuckDBType.DuckdbTypeVarchar;
            }
        }

        #endregion private method
    }
}