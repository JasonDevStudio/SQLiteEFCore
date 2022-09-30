namespace SQLiteLib.Table.Interfaces
{
    /// <summary>
    /// IDataColumnCollection
    /// </summary>
    public interface IDataColumnCollection : IList<IDataColumn>
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
        IDataColumn this[int index] { get; set; }

        /// <summary>
        /// 索引器
        /// </summary>
        /// <param name="field">字段名</param>
        /// <returns>object</returns>
        IDataColumn this[string field] { get; }
    }
}
