using Hdf5Lib.Table.Interfaces;

namespace Hdf5Lib.Table.Impl
{
    /// <summary>
    /// IDataRow
    /// </summary>
    public partial class DataRow : IDataRow
    {
        /// <summary>
        /// DataTable
        /// </summary>
        [Newtonsoft.Json.JsonIgnore]
        public IDataTable Table { get; set; }

        /// <summary>
        /// Row Index
        /// </summary>
        public int RowIndex { get; set; }

        /// <summary>
        /// 数据结果集合
        /// </summary>
        public object[] Values { get; set; }

        /// <summary>
        /// 索引器
        /// </summary>
        /// <param name="index">列索引</param>
        /// <returns>object</returns>
        public object this[int index]
        {
            get => Values[index];
            set => Values[index] = value;
        }

        /// <summary>
        /// 索引器
        /// </summary>
        /// <param name="field">字段名</param>
        /// <returns>object</returns>
        public object this[string field]
        {
            get => this[this.Table.Columns[field]];
            set => this[this.Table.Columns[field]] = value;
        }

        /// <summary>
        /// 索引器
        /// </summary>
        /// <param name="column">IDataColumn</param>
        /// <returns>object</returns>
        public object this[IDataColumn column]
        {
            get => this.Values[column.ColumnIndex];
            set => this.Values[column.ColumnIndex] = value;
        }
    }
}