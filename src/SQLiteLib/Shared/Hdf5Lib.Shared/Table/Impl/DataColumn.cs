using Hdf5Lib.Table.Interfaces;

namespace Hdf5Lib.Table.Impl
{
    /// <summary>
    /// IDataColumn
    /// </summary>
    public partial class DataColumn : IDataColumn
    {
        /// <summary>
        /// DataTable
        /// </summary>
        [Newtonsoft.Json.JsonIgnore]
        public IDataTable Table { get; set; }

        /// <summary>
        /// Id
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// Name
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// 是否主键
        /// </summary>
        public bool IsPK { get; set; }

        /// <summary>
        /// 是否自增
        /// </summary>
        public bool IsAutoincrement { get; set; }

        /// <summary>
        /// Origin Table ID
        /// </summary>
        public string OriginTableId { get; set; }

        /// <summary>
        /// Field
        /// </summary>
        public string Field { get; set; }

        /// <summary>
        /// Column Origin Index
        /// </summary>
        public int ColumnIndex { get; set; }

        /// <summary>
        /// Column Dispaly Index
        /// </summary>
        public int VisbleIndex { get; set; }

        /// <summary>
        /// Data type
        /// </summary>
        public TypeCode TypeCode { get; set; }

        /// <summary>
        /// 表达式
        /// </summary>
        public string Expression { get; set; }

        /// <summary>
        /// 排序方式
        /// </summary>
        public string OrderBy { get; set; }
    }
}