namespace SQLiteLib.Table.Impl
{
    /// <summary>
    /// IDataColumn
    /// </summary>
    public class DataColumn : IDataColumn
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
        public short ColumnIndex { get; set; }

        /// <summary>
        /// Column Dispaly Index
        /// </summary>
        public short VisbleIndex { get; set; }

        /// <summary>
        /// Data type
        /// </summary>
        public TypeCode TypeCode { get; set; }

        /// <summary>
        /// 表达式
        /// </summary>
        public string Expression { get; set; }
    }
}
