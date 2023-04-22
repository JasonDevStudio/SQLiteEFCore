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
        /// Initializes a new instance of the <see cref="DataColumnCollection"/> class.
        /// </summary>
        /// <param name="columns">The columns.</param>
        /// <param name="table">The table.</param>
        public DataColumnCollection(List<IDataColumn> columns, IDataTable table = null)
        {
            this.Table = table;
            this.AddRange(columns);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DataColumnCollection"/> class.
        /// </summary>
        /// <param name="columns">The columns.</param>
        /// <param name="table">The table.</param>
        public DataColumnCollection(IDataColumnCollection columns, IDataTable table = null) : this(columns.Columns, table)
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
        public IDataColumnCollection Clone()
        {
            var columns = new DataColumnCollection{Table = this.Table};

            foreach (var column in this.Columns)
            {
                var newColumn = new DataColumn(column);
                columns.Add(newColumn);
            }
             
            return columns;
        }

        /// <summary>
        /// 添加数据列
        /// </summary>
        /// <param name="column">IDataColumn</param> 
        public void Add(IDataColumn column)
        {
            if (!(this.Columns?.Any(col => column.Field == col.Field) ?? false))
            {
                var newColumn = new DataColumn(column) { ColumnIndex = this.Count };
                this.Columns.Add(newColumn);
                this.DicColumns[column.Field] = column;
            }
        }

        /// <summary>
        /// 批量添加数据列
        /// </summary>
        /// <param name="columns">数据列集合</param> 
        public void AddRange(IEnumerable<IDataColumn> columns)
        {
            var index = 0;

            if (columns?.Any() ?? false)
            {
                foreach (var col in columns)
                {
                    if (!(this.Columns?.Any(c => c.Field == col.Field) ?? false))
                    {
                        var newColumn = new DataColumn(col) { ColumnIndex = this.Count };
                        this.Columns.Add(newColumn);
                    }
                }

                this.DicColumns = this.Columns.ToDictionary(m => m.Field, m => m);
            }
        }

        /// <summary>
        /// 批量添加列
        /// </summary>
        /// <param name="columns">数据列集合</param>
        public void AddRange(IDataColumnCollection columns) => this.AddRange(columns.Columns);

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

        /// <summary>
        /// Converts to dictionary.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TElement">The type of the element.</typeparam> 
        /// <param name="keySelector">The key selector.</param>
        /// <param name="elementSelector">The element selector.</param>
        /// <returns>Dictionary</returns>
        public Dictionary<TKey, TElement> ToDictionary<TKey, TElement>(Func<IDataColumn, TKey> keySelector, Func<IDataColumn, TElement> elementSelector) where TKey : notnull =>
            this.Columns.ToDictionary(keySelector, elementSelector);

    }
}