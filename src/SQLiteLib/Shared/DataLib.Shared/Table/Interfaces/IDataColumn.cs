namespace DataLib.Table.Interfaces
{
    /// <summary>
    /// IDataColumn
    /// </summary>
    public interface IDataColumn
    { 
        /// <summary>
        /// DataTable
        /// </summary>
        [Newtonsoft.Json.JsonIgnore]
        IDataTable Table { get; set; }

        /// <summary>
        /// Id
        /// </summary>
        string Id { get; set; }

        /// <summary>
        /// Name
        /// </summary>
        string Name { get; set; }

        /// <summary>
        /// 是否主键
        /// </summary>
        bool IsPK { get; set; }

        /// <summary>
        /// 是否自增
        /// </summary>
        bool IsAutoincrement { get; set; }

        /// <summary>
        /// Origin Table ID
        /// </summary>
        string OriginTableId { get; set; }

        /// <summary>
        /// Field
        /// </summary>
        string Field { get; set; }

        /// <summary>
        /// OrderBy
        /// </summary>
        string OrderBy { get; set; }

        /// <summary>
        /// Column Origin Index
        /// </summary>
        int ColumnIndex { get; set; }

        /// <summary>
        /// Column Dispaly Index
        /// </summary>
        int VisbleIndex { get; set; }

        /// <summary>
        /// Data type
        /// </summary>
        TypeCode TypeCode { get; set; }

        /// <summary>
        /// 表达式
        /// </summary>
        string Expression { get; set; }

        /// <summary>
        /// Gets or sets the values.
        /// </summary>
        /// <value>
        /// The values.
        /// </value>
        Array Values { get; set; }
    }
}