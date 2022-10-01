using SQLiteLib.Table.Interfaces;

namespace SQLiteLib.Table.Impl
{
    /// <summary>
    /// IDataColumnCollection
    /// </summary>
    public class DataColumnCollection : List<IDataColumn>, IDataColumnCollection
    {
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
    }
}
