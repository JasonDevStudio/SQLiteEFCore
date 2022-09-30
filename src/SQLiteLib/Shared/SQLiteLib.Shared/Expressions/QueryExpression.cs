using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace SQLiteEFCore.Shared.DB
{
    public abstract class QueryExpression
    {
        public string Field { get; set; }

        /// <summary>
        /// 一元运算逻辑
        /// </summary>
        public string Logic { get; set; }

        /// <summary>
        /// 值
        /// </summary>
        public object Value { get; set; }

        /// <summary>
        /// 生成Sql
        /// </summary>
        /// <returns>Sql</returns>
        public abstract string BuildSql();
    }
}
