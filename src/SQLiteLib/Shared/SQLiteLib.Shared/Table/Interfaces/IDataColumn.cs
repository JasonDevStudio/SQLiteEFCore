namespace SQLiteLib.Table.Interfaces
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
        short ColumnIndex { get; set; }

        /// <summary>
        /// Column Dispaly Index
        /// </summary>
        short VisbleIndex { get; set; }

        /// <summary>
        /// Data type
        /// </summary>
        TypeCode TypeCode { get; set; }

        /// <summary>
        /// 表达式
        /// </summary>
        string Expression { get; set; } 
    }
}
