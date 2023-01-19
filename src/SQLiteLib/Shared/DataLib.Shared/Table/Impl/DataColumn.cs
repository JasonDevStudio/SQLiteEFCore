using DataLib.Table.Interfaces;

namespace DataLib.Table.Impl
{
    /// <summary>
    /// IDataColumn
    /// </summary>
    public partial class DataColumn : IDataColumn
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DataColumn"/> class.
        /// </summary>
        public DataColumn()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DataColumn"/> class.
        /// </summary>
        /// <param name="column">The orig column.</param>
        public DataColumn(IDataColumn column)
        {
            this.Id = column.Id;
            this.Name= column.Name;
            this.OrderBy = column.OrderBy;
            this.Expression = column.Expression;
            this.ColumnIndex = column.ColumnIndex;
            this.VisbleIndex = column.VisbleIndex;
            this.Table = column.Table;
            this.Field = column.Field;
            this.IsPK = column.IsPK;
            this.IsAutoincrement = column.IsAutoincrement;
            this.OriginTableId = column.OriginTableId;
            this.TypeCode = column.TypeCode;
        }

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