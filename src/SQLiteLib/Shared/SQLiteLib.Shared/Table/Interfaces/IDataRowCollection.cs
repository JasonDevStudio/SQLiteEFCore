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

        /// <summary>
        /// 批量添加行
        /// </summary>
        /// <param name="rows">数据行集合</param>
        void AddRange(IEnumerable<IDataRow> rows);

        /// <summary>
        /// Copies the entire System.Collections.Generic.List`1 to a compatible one-dimensional array, starting at the beginning of the target array.
        /// </summary>
        IDataRowCollection Copy();
    }
}