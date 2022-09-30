namespace SQLiteLib.Table.Interfaces
{
    /// <summary>
    /// IDataColumnCollection
    /// </summary>
    public interface IDataRowCollection : IList<IDataRow>
    {
        /// <summary>
        /// DataTable
        /// </summary>
        [Newtonsoft.Json.JsonIgnore]
        IDataTable Table { get; set; }

        /// <summary>
        /// 索引器
        /// </summary>
        /// <param name="index">列索引</param>
        /// <returns>object</returns>
        IDataRow this[int index] { get; set; }
    }
}
