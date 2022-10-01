namespace SQLiteLib.Table.Interfaces
{
    /// <summary>
    /// IDataTable
    /// </summary>
    public interface IDataTable
    {
        /// <summary>
        /// Id
        /// </summary>
        string Id { get; set; }

        /// <summary>
        /// Name
        /// </summary>
        string Name { get; set; }

        /// <summary>
        /// 数据行数量
        /// </summary>
        int RowCount { get; }

        /// <summary>
        /// 数据列数量
        /// </summary>
        int ColumnCount { get; }

        /// <summary>
        /// 数据列集合
        /// </summary>
        IDataColumnCollection Columns { get; set; }

        /// <summary>
        /// 数据行集合
        /// </summary>
        IDataRowCollection Rows { get; set; }

        /// <summary>
        /// 获取数据行
        /// </summary>
        /// <param name="index">数据行索引</param>
        /// <returns>IDataRow</returns>
        IDataRow this[int index] { get; }

        /// <summary>
        /// 刷新图表
        /// </summary>
        void Reflash();

        /// <summary>
        /// 新增数据
        /// </summary>
        void BulkInsert();

        /// <summary>
        /// 创建新数据行
        /// </summary>
        /// <returns>IDataRow</returns>
        IDataRow NewRow();
    }
}
