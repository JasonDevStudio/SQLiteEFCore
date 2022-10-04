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

        /// <summary>
        /// 批量添加列
        /// </summary>
        /// <param name="columns">数据列集合</param>
        void AddRange(IEnumerable<IDataColumn> columns);

        /// <summary>
        /// Copies the entire System.Collections.Generic.List`1 to a compatible one-dimensional array, starting at the beginning of the target array.
        /// </summary>
        IDataColumnCollection Copy();
    }
}