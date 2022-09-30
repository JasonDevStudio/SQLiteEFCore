using SQLiteLib.Table.Interfaces;

namespace SQLiteLib.Table.Impl
{
    /// <summary>
    /// IDataColumnCollection
    /// </summary>
    public class DataRowCollection : List<IDataRow>, IDataRowCollection
    {
        /// <summary>
        /// DataTable
        /// </summary>
        [Newtonsoft.Json.JsonIgnore]
        public IDataTable Table { get; set; }
    }
}
