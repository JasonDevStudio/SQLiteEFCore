namespace SQLiteLib.Table.Impl
{
    /// <summary>
    /// IDataTable
    /// </summary>
    public class DataTable : IDataTable
    {
        /// <summary>
        /// Id
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// Name
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// 数据行数量
        /// </summary>
        public int RowCount { get; }

        /// <summary>
        /// 数据列数量
        /// </summary>
        public int ColumnCount { get; }

        /// <summary>
        /// 数据列集合
        /// </summary>
        public IDataColumnCollection Columns { get; set; }

        /// <summary>
        /// 数据行集合
        /// </summary>
        public IDataRowCollection Rows { get; set; }

        /// <summary>
        /// 获取数据行
        /// </summary>
        /// <param name="index">数据行索引</param>
        /// <returns>IDataRow</returns>
        public IDataRow this[int index] => this.Rows[index];

        /// <summary>
        /// 刷新图表
        /// </summary>
        public void Reflash() { }

        /// <summary>
        /// 新增数据
        /// </summary>
        public void BulkInsert() { }
    }
}
