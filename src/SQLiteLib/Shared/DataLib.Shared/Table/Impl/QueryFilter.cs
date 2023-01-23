using DataLib.Table.Interfaces;

namespace DataLib.Table
{
    public abstract class QueryFilter : IQueryFilter
    {
        /// <summary>
        /// 数据列
        /// </summary>
        public IDataColumn DataColumn { get; set; }

        /// <summary>
        /// 一元运算逻辑
        /// </summary>
        public string Logic { get; set; }

        /// <summary>
        /// 值
        /// </summary>
        public object Value { get; set; }

        /// <summary>
        /// 二元运算逻辑
        /// </summary>
        public string Binary { get; set; }

        /// <summary>
        /// 生成Sql
        /// </summary>
        /// <returns>Sql</returns>
        public abstract string ToString();

        /// <summary>
        /// Filters the asynchronous.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception> 
        public abstract List<int> Filter(Array source);
    }
}