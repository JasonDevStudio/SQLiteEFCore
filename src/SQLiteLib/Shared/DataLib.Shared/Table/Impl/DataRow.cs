using System;
using DataLib.Table.Interfaces;

namespace DataLib.Table.Impl
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
        public Array Values { get; set; }

        /// <summary>
        /// 索引器
        /// </summary>
        /// <param name="index">列索引</param>
        /// <returns>object</returns>
        public object this[int index]
        {
            get => Values.GetValue(index);
            set => Values.SetValue(value, index);
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
            get => Values.GetValue(column.ColumnIndex);
            set => Values.SetValue(value, column.ColumnIndex);
        }
    }
}