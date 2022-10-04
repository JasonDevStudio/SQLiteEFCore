using SQLiteLib.Table.Interfaces;

namespace SQLiteLib.Table.Impl
{
    /// <summary>
    /// IDataColumnCollection
    /// </summary>
    public partial class DataRowCollection : List<IDataRow>, IDataRowCollection
    {
        /// <summary>
        /// DataRowCollection
        /// </summary>
        public DataRowCollection()
        {
        }

        /// <summary>
        /// DataRowCollection
        /// </summary>
        /// <param name="table">IDataTable</param>
        public DataRowCollection(IDataTable table) : this()
        {
            this.Table = table;
        }

        /// <summary>
        /// DataRowCollection
        /// </summary>
        /// <param name="table">IDataTable</param>
        /// <param name="rows">数据行</param>
        public DataRowCollection(IDataTable table, List<IDataRow> rows) : this()
        {
            this.Table = table;
            this.AddRange(rows);
        }

        /// <summary>
        /// DataTable
        /// </summary>
        [Newtonsoft.Json.JsonIgnore]
        public IDataTable Table { get; set; }

        /// <summary>
        /// 复制数据行
        /// </summary>
        /// <returns>IDataRowCollection</returns>
        public IDataRowCollection Copy()
        {
            var rows = new DataRowCollection(this.Table);
            var rowArry = new IDataRow[this.Count];
            this.CopyTo(rowArry, 0);
            rows.AddRange(rowArry);
            return rows;
        }
    }
}