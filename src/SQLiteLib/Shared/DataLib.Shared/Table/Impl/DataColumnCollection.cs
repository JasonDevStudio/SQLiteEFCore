using System.Data.Common;
using System.Linq;
using DataLib.Table.Interfaces;

namespace DataLib.Table.Impl
{
    /// <summary>
    /// IDataColumnCollection
    /// </summary>
    public partial class DataColumnCollection : IDataColumnCollection
    {
        private Dictionary<string, IDataColumn> DicColumns = new Dictionary<string, IDataColumn>();

        /// <summary>
        /// DataColumnCollection
        /// </summary>
        public DataColumnCollection()
        {
        }
         
        /// <summary>
        /// 数据列集合
        /// </summary>
        public List<IDataColumn> Columns { get; private set; } = new List<IDataColumn>();

        /// <summary>
        /// DataTable
        /// </summary>
        [Newtonsoft.Json.JsonIgnore]
        public IDataTable Table { get; set; }

        /// <summary>
        /// 数据列数量
        /// </summary>
        public int Count => this.Columns.Count;

        /// <summary>
        /// 索引器
        /// </summary>
        /// <param name="index">索引</param>
        /// <returns>IDataColumn</returns> 
        public IDataColumn this[int index]
        {
            get => this.Columns[index];
            set => this.Columns[index] = value;
        }

        /// <summary>
        /// 索引器
        /// </summary>
        /// <param name="field">字段名</param>
        /// <returns>object</returns>
        public IDataColumn this[string field] => this.DicColumns[field];

        /// <summary>
        /// 复制数据列集合
        /// </summary>
        /// <returns>IDataColumnCollection</returns>
        public IDataColumnCollection Copy()
        {
            var columns = GlobalService.GetService<IDataColumnCollection>();
            var colArry = new IDataColumn[this.Count];
            this.Columns.CopyTo(colArry, 0);
            columns.AddRange(colArry);
            columns.Table = this.Table; 
            return columns;
        }

        /// <summary>
        /// 添加数据列
        /// </summary>
        /// <param name="column">IDataColumn</param> 
        public void Add(IDataColumn column)
        {
            this.Columns.Add(column);
            this.DicColumns[column.Field] = column;
        }

        /// <summary>
        /// 批量添加数据列
        /// </summary>
        /// <param name="columns">数据列集合</param> 
        public void AddRange(IEnumerable<IDataColumn> columns)
        {
            this.Columns.AddRange(columns);
            this.DicColumns = this.Columns.ToDictionary(m=>m.Field, m=>m);
        }

        /// <summary>
        /// 批量添加列
        /// </summary>
        /// <param name="columns">数据列集合</param>
        public void AddRange(IDataColumnCollection columns)
        {
            this.Columns.AddRange(columns.Columns);
            this.DicColumns = this.Columns.ToDictionary(m => m.Field, m => m);
        }

        /// <summary>
        /// Where
        /// </summary>
        /// <param name="predicate">Func{IDataColumn, bool}</param>
        /// <returns>IEnumerable{IDataColumn}</returns>
        public IEnumerable<IDataColumn> Where(Func<IDataColumn, bool> predicate) => this.Columns.Where(predicate);

        /// <summary>
        /// Where
        /// </summary>
        /// <param name="predicate">Func{IDataColumn,int, bool}</param>
        /// <returns>IEnumerable{IDataColumn}</returns>
        public IEnumerable<IDataColumn> Where(Func<IDataColumn, int, bool> predicate) => this.Columns.Where(predicate);

        /// <summary>
        /// Select
        /// </summary>
        /// <typeparam name="TResult">查询结果数据类型</typeparam>
        /// <param name="selector">选择器</param>
        /// <returns>IEnumerable{TResult}</returns>
        public IEnumerable<TResult> Select<TResult>(Func<IDataColumn, TResult> selector) => this.Columns.Select(selector);

        /// <summary>
        /// OrderBy
        /// </summary>
        /// <typeparam name="TKey">排序数据类型</typeparam>
        /// <param name="keySelector">Key Selector</param>
        /// <returns>IOrderedEnumerable</returns>
        public IOrderedEnumerable<IDataColumn> OrderBy<TKey>(Func<IDataColumn, TKey> keySelector) => this.Columns.OrderBy(keySelector);

        /// <summary>
        /// OrderByDescending
        /// </summary>
        /// <typeparam name="TKey">排序数据类型</typeparam>
        /// <param name="keySelector">Key Selector</param>
        /// <returns>IOrderedEnumerable</returns>
        public IOrderedEnumerable<IDataColumn> OrderByDescending<TKey>(Func<IDataColumn, TKey> keySelector) => this.Columns.OrderByDescending(keySelector);

        /// <summary>
        /// GroupBy
        /// </summary>
        /// <typeparam name="TKey">分组KEY 数据类型</typeparam>
        /// <param name="keySelector">分组选择器</param>
        /// <returns>IEnumerable{IGrouping{TKey, IDataColumn}}</returns>
        public IEnumerable<IGrouping<TKey, IDataColumn>> GroupBy<TKey>(Func<IDataColumn, TKey> keySelector) => this.Columns.GroupBy(keySelector);

        /// <summary>
        /// Any
        /// </summary>  
        /// <returns>bool</returns>
        public bool Any() => this.Columns.Any();

        /// <summary>
        /// Any
        /// </summary> 
        /// <param name="predicate">Func{IDataColumn, bool}</param>
        /// <returns>bool</returns>
        public bool Any(Func<IDataColumn, bool> predicate) => this.Columns.Any(predicate);

        /// <summary>
        /// All
        /// </summary> 
        /// <param name="predicate">Func{IDataColumn, bool}</param>
        /// <returns>bool</returns>
        public bool All(Func<IDataColumn, bool> predicate) => this.Columns.All(predicate);

        /// <summary>
        /// 遍历函数
        /// </summary>
        /// <param name="action">Action</param>
        public void ForEach(Action<IDataColumn> action) => this.Columns.ForEach(action);
    }
}