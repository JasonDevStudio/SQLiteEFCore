namespace DataLib.Table.Interfaces
{
    /// <summary>
    /// IDataRow
    /// </summary>
    public interface IDataRow
    {
        /// <summary>
        /// DataTable
        /// </summary>
        [Newtonsoft.Json.JsonIgnore]
        IDataTable Table { get; set; }

        /// <summary>
        /// Row Index
        /// </summary>
        int RowIndex { get; set; }

        /// <summary>
        /// 数据结果集合
        /// </summary>
        Array Values { get; set; }

        /// <summary>
        /// 索引器
        /// </summary>
        /// <param name="index">列索引</param>
        /// <returns>object</returns>
        object this[int index] { get; set; }

        /// <summary>
        /// 索引器
        /// </summary>
        /// <param name="field">字段名</param>
        /// <returns>object</returns>
        object this[string field] { get; set; }

        /// <summary>
        /// 索引器
        /// </summary>
        /// <param name="column">IDataColumn</param>
        /// <returns>object</returns>
        object this[IDataColumn column] { get; set; }
    }
}