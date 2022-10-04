using SQLiteLib.Table.Interfaces;

namespace SQLiteLib.Table.Impl
{
    /// <summary>
    /// IDataColumnCollection
    /// </summary>
    public partial class DataColumnCollection : List<IDataColumn>, IDataColumnCollection
    {
        /// <summary>
        /// DataColumnCollection
        /// </summary>
        public DataColumnCollection()
        {
        }

        /// <summary>
        /// DataColumnCollection
        /// </summary>
        /// <param name="table">IDataTable</param>
        public DataColumnCollection(IDataTable table) : this()
        {
            this.Table = table;
        }

        /// <summary>
        /// DataColumnCollection
        /// </summary>
        /// <param name="table">IDataTable</param>
        /// <param name="columns">数据列集合</param>
        public DataColumnCollection(IDataTable table, List<IDataColumn> columns) : this()
        {
            this.Table = table;
            this.AddRange(columns);
        }

        /// <summary>
        /// DataTable
        /// </summary>
        [Newtonsoft.Json.JsonIgnore]
        public IDataTable Table { get; set; }

        /// <summary>
        /// 索引器
        /// </summary>
        /// <param name="field">字段名</param>
        /// <returns>object</returns>
        public IDataColumn this[string field] => this.FirstOrDefault(m => m.Field == field);

        /// <summary>
        /// 复制数据列集合
        /// </summary>
        /// <returns>IDataColumnCollection</returns>
        public IDataColumnCollection Copy()
        {
            var columns = new DataColumnCollection(this.Table);
            var colArry = new IDataColumn[this.Count];
            this.CopyTo(colArry, 0);
            columns.AddRange(colArry);
            return columns;
        }
    }
}