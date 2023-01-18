namespace DataLib.Table.Interfaces
{
    /// <summary>
    /// IDataColumnCollection
    /// </summary>
    public interface IDataColumnCollection
    {
        /// <summary>
        /// DataTable
        /// </summary>
        [Newtonsoft.Json.JsonIgnore]
        IDataTable Table { get; set; }

        /// <summary>
        /// 数据列集合
        /// </summary>
        List<IDataColumn> Columns { get; }

        /// <summary>
        /// 数量
        /// </summary>
        int Count { get; }

        /// <summary>
        /// 索引器
        /// </summary>
        /// <param name="index">列索引</param>
        /// <returns>object</returns>
        IDataColumn this[int index] { get; set; }

        /// <summary>
        /// 索引器
        /// </summary>
        /// <param name="field">字段名</param>
        /// <returns>object</returns>
        IDataColumn this[string field] { get; }

        /// <summary>
        /// 添加列
        /// </summary>
        /// <param name="column">数据列</param>
        void Add(IDataColumn column);

        /// <summary>
        /// 批量添加列
        /// </summary>
        /// <param name="columns">数据列集合</param>
        void AddRange(IEnumerable<IDataColumn> columns);

        /// <summary>
        /// 批量添加列
        /// </summary>
        /// <param name="columns">数据列集合</param>
        void AddRange(IDataColumnCollection columns);

        /// <summary>
        /// 复制数据列举集合
        /// Copies the entire System.Collections.Generic.List`1 to a compatible one-dimensional array, starting at the beginning of the target array.
        /// </summary>
        IDataColumnCollection Copy();

        /// <summary>
        /// Where
        /// </summary>
        /// <param name="predicate">Func{IDataColumn, bool}</param>
        /// <returns>IEnumerable{IDataColumn}</returns>
        IEnumerable<IDataColumn> Where(Func<IDataColumn, bool> predicate);

        /// <summary>
        /// Where
        /// </summary>
        /// <param name="predicate">Func{IDataColumn,int, bool}</param>
        /// <returns>IEnumerable{IDataColumn}</returns>
        IEnumerable<IDataColumn> Where(Func<IDataColumn, int, bool> predicate);

        /// <summary>
        /// Select
        /// </summary>
        /// <typeparam name="TResult">查询结果数据类型</typeparam>
        /// <param name="selector">选择器</param>
        /// <returns>IEnumerable{TResult}</returns>
        IEnumerable<TResult> Select<TResult>(Func<IDataColumn, TResult> selector);

        /// <summary>
        /// OrderBy
        /// </summary>
        /// <typeparam name="TKey">排序数据类型</typeparam>
        /// <param name="keySelector">Key Selector</param>
        /// <returns>IOrderedEnumerable</returns>
        IOrderedEnumerable<IDataColumn> OrderBy<TKey>(Func<IDataColumn, TKey> keySelector);

        /// <summary>
        /// OrderByDescending
        /// </summary>
        /// <typeparam name="TKey">排序数据类型</typeparam>
        /// <param name="keySelector">Key Selector</param>
        /// <returns>IOrderedEnumerable</returns>
        IOrderedEnumerable<IDataColumn> OrderByDescending<TKey>(Func<IDataColumn, TKey> keySelector);

        /// <summary>
        /// GroupBy
        /// </summary>
        /// <typeparam name="TKey">分组KEY 数据类型</typeparam>
        /// <param name="keySelector">分组选择器</param>
        /// <returns>IEnumerable{IGrouping{TKey, IDataColumn}}</returns>
        IEnumerable<IGrouping<TKey, IDataColumn>> GroupBy<TKey>(Func<IDataColumn, TKey> keySelector);

        /// <summary>
        /// Any
        /// </summary>  
        /// <returns></returns>
        bool Any();

        /// <summary>
        /// Any
        /// </summary> 
        /// <param name="predicate">Func{IDataColumn, bool}</param>
        /// <returns>bool</returns>
        bool Any(Func<IDataColumn, bool> predicate);

        /// <summary>
        /// All
        /// </summary> 
        /// <param name="predicate">Func{IDataColumn, bool}</param>
        /// <returns>bool</returns>
        bool All(Func<IDataColumn, bool> predicate);

        /// <summary>
        /// 遍历函数
        /// </summary>
        /// <param name="action">Action</param>
        void ForEach(Action<IDataColumn> action);
    }
}