using System;
using DataLib.Table.Interfaces;

namespace DataLib.Table.Impl
{
    /// <summary>
    /// IDataColumnCollection
    /// </summary>
    public partial class DataRowCollection : IDataRowCollection
    {
        /// <summary>
        /// DataRowCollection
        /// </summary>
        public DataRowCollection()
        {
        }

        /// <summary>
        /// 索引器
        /// </summary>
        /// <param name="index">索引</param>
        /// <returns>IDataRow</returns>
        public IDataRow this[int index]
        {
            get => this.Rows[index];
            set => this.Rows[index] = value;
        }

        /// <summary>
        /// DataTable
        /// </summary>
        [Newtonsoft.Json.JsonIgnore]
        public IDataTable Table { get; set; }

        /// <summary>
        /// 数量
        /// </summary>
        public int Count => this.Rows.Count;

        /// <summary>
        /// 数据行集合
        /// </summary>
        public List<IDataRow> Rows { get; private set; } = new List<IDataRow>();

        /// <summary>
        /// 添加数据行
        /// </summary>
        /// <param name="row">数据行</param> 
        public void Add(IDataRow row) => this.Rows.Add(row);

        /// <summary>
        /// 批量添加数据行
        /// </summary>
        /// <param name="rows">数据行集合</param> 
        public void AddRange(IEnumerable<IDataRow> rows) => this.Rows.AddRange(rows);

        /// <summary>
        /// 批量添加行
        /// </summary>
        /// <param name="rows">数据行集合</param>
        public void AddRange(IDataRowCollection rows) => this.Rows.AddRange(rows.Rows);

        /// <summary>
        /// 复制数据行
        /// </summary>
        /// <returns>IDataRowCollection</returns>
        public IDataRowCollection Copy()
        {
            var rows = GlobalService.GetService<IDataRowCollection>();
            var rowArry = new IDataRow[this.Count];
            this.Rows.CopyTo(rowArry, 0);
            rows.AddRange(rowArry);
            rows.Table = this.Table;
            return rows;
        }

        /// <summary>
        /// Where
        /// </summary>
        /// <param name="predicate">Func{IDataRow, bool}</param>
        /// <returns>IEnumerable{IDataRow}</returns>
        public IEnumerable<IDataRow> Where(Func<IDataRow, bool> predicate) => this.Rows.Where(predicate);

        /// <summary>
        /// Where
        /// </summary>
        /// <param name="predicate">Func{IDataRow,int, bool}</param>
        /// <returns>IEnumerable{IDataRow}</returns>
        public IEnumerable<IDataRow> Where(Func<IDataRow, int, bool> predicate) => this.Rows.Where(predicate);

        /// <summary>
        /// Select
        /// </summary>
        /// <typeparam name="TResult">查询结果数据类型</typeparam>
        /// <param name="selector">选择器</param>
        /// <returns>IEnumerable{TResult}</returns>
        public IEnumerable<TResult> Select<TResult>(Func<IDataRow, TResult> selector) => this.Rows.Select(selector);

        /// <summary>
        /// OrderBy
        /// </summary>
        /// <typeparam name="TKey">排序数据类型</typeparam>
        /// <param name="keySelector">Key Selector</param>
        /// <returns>IOrderedEnumerable</returns>
        public IOrderedEnumerable<IDataRow> OrderBy<TKey>(Func<IDataRow, TKey> keySelector) => this.Rows.OrderBy(keySelector);

        /// <summary>
        /// OrderByDescending
        /// </summary>
        /// <typeparam name="TKey">排序数据类型</typeparam>
        /// <param name="keySelector">Key Selector</param>
        /// <returns>IOrderedEnumerable</returns>
        public IOrderedEnumerable<IDataRow> OrderByDescending<TKey>(Func<IDataRow, TKey> keySelector) => this.Rows.OrderByDescending(keySelector);

        /// <summary>
        /// GroupBy
        /// </summary>
        /// <typeparam name="TKey">分组KEY 数据类型</typeparam>
        /// <param name="keySelector">分组选择器</param>
        /// <returns>IEnumerable{IGrouping{TKey, IDataRow}}</returns>
        public IEnumerable<IGrouping<TKey, IDataRow>> GroupBy<TKey>(Func<IDataRow, TKey> keySelector) => this.Rows.GroupBy(keySelector);

        /// <summary>
        /// Any
        /// </summary>  
        /// <returns>bool</returns>
        public bool Any() => this.Rows.Any();

        /// <summary>
        /// Any
        /// </summary> 
        /// <param name="predicate">Func{IDataRow, bool}</param>
        /// <returns></returns>
        public bool Any(Func<IDataRow, bool> predicate) => this.Rows.Any(predicate);

        /// <summary>
        /// All
        /// </summary> 
        /// <param name="predicate">Func{IDataRow, bool}</param>
        /// <returns></returns>
        public bool All(Func<IDataRow, bool> predicate) => this.Rows.All(predicate);

        /// <summary>
        /// 遍历函数
        /// </summary>
        /// <param name="action">Action</param>
        public void ForEach(Action<IDataRow> action) => this.Rows.ForEach(action);
    }
}