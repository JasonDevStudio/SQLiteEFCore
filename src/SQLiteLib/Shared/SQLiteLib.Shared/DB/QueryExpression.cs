using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;
using System.Linq;
using System.Text.RegularExpressions;

namespace SQLiteEFCore.Shared.DB
{
    public class QueryExpression
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
        public virtual string BuildSql()
        {
            switch (this.Logic)
            {
                case QueryLogic.Like:
                    return this.BuildLikeSql();
                case QueryLogic.IN:
                    return this.BuildINSql();
                case QueryLogic.LessThanOrEqual:
                    return this.BuildLessThanOrEqualSql();
                case QueryLogic.LessThan:
                    return this.BuildLessThanSql();
                case QueryLogic.GreaterThanOrEqual:
                    return this.BuildGreaterThanOrEqualSql();
                case QueryLogic.GreaterThan:
                    return this.BuildGreaterThanSql();
                case QueryLogic.Between:
                    return this.BuildBetweenSql();
                case QueryLogic.NotBetween:
                    return this.BuildNotBetweenSql();
                case QueryLogic.IsNull:
                    return this.BuildIsNullSql();
                case QueryLogic.IsNotNull:
                    return this.BuildIsNotNullSql();
                case QueryLogic.NotEqual:
                    return this.BuildNotEqualSql();
                case QueryLogic.Equal:
                default:
                    return this.BuildEqualSql();
            }
        }

        /// <summary>
        /// Build Like Sql
        /// </summary>
        /// <returns>Sql</returns>
        private string BuildLikeSql() => $"{this.Field} {QueryLogic.Like} '%'{this.Value}'%' ";

        /// <summary>
        /// Build LessThanOrEqual(<=) Sql
        /// </summary>
        /// <returns>Sql</returns>
        private string BuildINSql()
        {
            if (this.Value is IList list && list.Count > 0)
            {
                var val = list[0];
                if (Regex.IsMatch($"{val}", @"^[-+]?([0-9]+)([.]([0-9]+))?$"))
                { 
                    var valStr = string.Join(',', list);
                    return $"{this.Field} {QueryLogic.IN} ({valStr}) ";
                }
                else
                {
                    var arry = new object[list.Count];
                    for (int i = 0; i < arry.Length; i++) 
                        arry[i] = $"'{list[i]}'";

                    var valStr = string.Join(',', arry);
                    return $"{this.Field} {QueryLogic.IN} ({valStr}) ";
                }
            }

            return string.Empty;
        }

        /// <summary>
        /// Build LessThanOrEqual(<=) Sql
        /// </summary>
        /// <returns>Sql</returns>
        private string BuildLessThanOrEqualSql() => $"{this.Field} {QueryLogic.LessThanOrEqual} {this.Value} ";

        /// <summary>
        /// Build LessThan(<) Sql
        /// </summary>
        /// <returns>Sql</returns>
        private string BuildLessThanSql() => $"{this.Field} {QueryLogic.LessThan} {this.Value} ";

        /// <summary>
        /// Build GreaterThanOrEqual(>=) Sql
        /// </summary>
        /// <returns>Sql</returns>
        private string BuildGreaterThanOrEqualSql() => $"{this.Field} {QueryLogic.GreaterThanOrEqual} {this.Value} ";

        /// <summary>
        /// Build GreaterThan(>) Sql
        /// </summary>
        /// <returns>Sql</returns>
        private string BuildGreaterThanSql() => $"{this.Field} {QueryLogic.GreaterThan} {this.Value} ";

        /// <summary>
        /// Build IsNull Sql
        /// </summary>
        /// <returns>Sql</returns>
        private string BuildIsNullSql() => $"{this.Field} {QueryLogic.IsNull} ";

        /// <summary>
        /// Build IsNotNull Sql
        /// </summary>
        /// <returns>Sql</returns>
        private string BuildIsNotNullSql() => $"{this.Field} {QueryLogic.IsNotNull} ";

        /// <summary>
        /// Build Equal Sql
        /// </summary>
        /// <returns>Sql</returns>
        private string BuildEqualSql() => $"{this.Field} {QueryLogic.Equal} '{this.Value}' ";

        /// <summary>
        /// Build NotEqual Sql
        /// </summary>
        /// <returns>Sql</returns>
        private string BuildNotEqualSql() => $"{this.Field} {QueryLogic.NotEqual} '{this.Value}' ";

        /// <summary>
        /// 生成Between Sql
        /// </summary>
        /// <returns>string</returns>
        private string BuildBetweenSql()
        {
            if (this.Value is IList list && list.Count == 2)
                return $"{this.Field} {QueryLogic.Between} {list[0]} {QueryLogic.AND} {list[^1]} ";
            return string.Empty;
        }

        /// <summary>
        /// 生成Not Between Sql
        /// </summary>
        /// <returns>string</returns>
        private string BuildNotBetweenSql()
        {
            if (this.Value is IList list && list.Count == 2)
                return $"{this.Field} {QueryLogic.NotBetween} {list[0]} {QueryLogic.AND} {list[^1]} ";
            return string.Empty;
        }
    }
}
