namespace Hdf5Lib.Table.Interfaces
{
    /// <summary>
    /// IDataColumnCollection
    /// </summary>
    public interface IDataRowCollection
    {
        /// <summary>
        /// DataTable
        /// </summary>
        [Newtonsoft.Json.JsonIgnore]
        IDataTable Table { get; set; }

        /// <summary>
        /// 数量
        /// </summary>
        int Count { get; }

        /// <summary>
        /// 数据行集合
        /// </summary>
        List<IDataRow> Rows { get; }

        /// <summary>
        /// 索引器
        /// </summary>
        /// <param name="index">列索引</param>
        /// <returns>object</returns>
        IDataRow this[int index] { get; set; }

        /// <summary>
        /// 添加行
        /// </summary>
        /// <param name="row">数据行</param>
        void Add(IDataRow row);

        /// <summary>
        /// 批量添加行
        /// </summary>
        /// <param name="rows">数据行集合</param>
        void AddRange(IEnumerable<IDataRow> rows);

        /// <summary>
        /// 批量添加行
        /// </summary>
        /// <param name="rows">数据行集合</param>
        void AddRange(IDataRowCollection rows);

        /// <summary>
        /// 复制数据行
        /// Copies the entire System.Collections.Generic.List`1 to a compatible one-dimensional array, starting at the beginning of the target array.
        /// </summary>
        IDataRowCollection Copy();

        /// <summary>
        /// Where
        /// </summary>
        /// <param name="predicate">Func{IDataRow, bool}</param>
        /// <returns>IEnumerable{IDataRow}</returns>
        IEnumerable<IDataRow> Where(Func<IDataRow, bool> predicate);

        /// <summary>
        /// Where
        /// </summary>
        /// <param name="predicate">Func{IDataRow,int, bool}</param>
        /// <returns>IEnumerable{IDataRow}</returns>
        IEnumerable<IDataRow> Where(Func<IDataRow, int, bool> predicate);

        /// <summary>
        /// Select
        /// </summary>
        /// <typeparam name="TResult">查询结果数据类型</typeparam>
        /// <param name="selector">选择器</param>
        /// <returns>IEnumerable{TResult}</returns>
        IEnumerable<TResult> Select<TResult>(Func<IDataRow, TResult> selector);

        /// <summary>
        /// OrderBy
        /// </summary>
        /// <typeparam name="TKey">排序数据类型</typeparam>
        /// <param name="keySelector">Key Selector</param>
        /// <returns>IOrderedEnumerable</returns>
        IOrderedEnumerable<IDataRow> OrderBy<TKey>(Func<IDataRow, TKey> keySelector);

        /// <summary>
        /// OrderByDescending
        /// </summary>
        /// <typeparam name="TKey">排序数据类型</typeparam>
        /// <param name="keySelector">Key Selector</param>
        /// <returns>IOrderedEnumerable</returns>
        IOrderedEnumerable<IDataRow> OrderByDescending<TKey>(Func<IDataRow, TKey> keySelector);

        /// <summary>
        /// GroupBy
        /// </summary>
        /// <typeparam name="TKey">分组KEY 数据类型</typeparam>
        /// <param name="keySelector">分组选择器</param>
        /// <returns>IEnumerable{IGrouping{TKey, IDataRow}}</returns>
        IEnumerable<IGrouping<TKey, IDataRow>> GroupBy<TKey>(Func<IDataRow, TKey> keySelector);

        /// <summary>
        /// Any
        /// </summary>  
        /// <returns></returns>
        bool Any();

        /// <summary>
        /// Any
        /// </summary> 
        /// <param name="predicate">Func{IDataRow, bool}</param>
        /// <returns></returns>
        bool Any(Func<IDataRow, bool> predicate);

        /// <summary>
        /// All
        /// </summary> 
        /// <param name="predicate">Func{IDataRow, bool}</param>
        /// <returns></returns>
        bool All(Func<IDataRow, bool> predicate);

        /// <summary>
        /// 遍历函数
        /// </summary>
        /// <param name="action">Action</param>
        void ForEach(Action<IDataRow> action);
    }
}