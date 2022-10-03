using SQLiteLib.Table.Interfaces;

namespace SQLiteLib
{
    /// <summary>
    /// QuerySetting
    /// </summary>
    public class QuerySetting
    {
        /// <summary>
        /// 需要查询数据的表
        /// </summary>
        public IDataTable Table { get; set; }

        /// <summary>
        /// 需要查询的数据列
        /// </summary>
        public IDataColumnCollection Columns { get; set; }

        /// <summary>
        /// 参与排序的列
        /// </summary>
        public IDataColumnCollection OrderFields { get; set; }

        /// <summary>
        /// 查询条件参数
        /// </summary>
        public List<Condition> Parameters { get; set; }
    }
}
