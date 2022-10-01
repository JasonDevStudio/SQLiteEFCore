using SQLiteLib.Table.Interfaces;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace SQLiteEFCore.Shared.DB
{
    public class Condition
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
        /// <param name="parameters">参数集合</param>
        /// <returns>SQL</returns>
        public static string BuildSql(List<Condition> parameters)
        {
            var sql = new StringBuilder();

            if (parameters.Any())
            {
                sql.Append("WHERE ");

                for (int i = 0; i < parameters.Count; i++)
                {
                    if (i > 0)
                        sql.Append($" {parameters[i].Binary} ");

                    sql.Append(parameters[i].ToString());
                }
            }

            return sql.ToString();
        }

        /// <summary>
        /// 生成排序SQL
        /// </summary>
        /// <param name="columns">需要排序的字段</param>
        /// <returns>SQL</returns>
        public static string BuildOrderSql(List<IDataColumn> columns)
        {
            var sql = new StringBuilder();

            if (columns?.Any() ?? false)
            {
                for (int i = 0; i < columns.Count; i++)
                {
                    if (i > 0)
                        sql.Append(',');

                    sql.Append($"{columns[i].Field} {columns[i].OrderBy} ");
                }
            }

            return sql.ToString();
        }

        /// <summary>
        /// 生成Sql
        /// </summary>
        /// <returns>Sql</returns>
        public virtual string ToString()
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
        private string BuildLikeSql() => $"{this.DataColumn.Field} {QueryLogic.Like} '%'{this.Value}'%' ";

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
                    return $"{this.DataColumn.Field} {QueryLogic.IN} ({valStr}) ";
                }
                else
                {
                    var arry = new object[list.Count];
                    for (int i = 0; i < arry.Length; i++)
                        arry[i] = $"'{list[i]}'";

                    var valStr = string.Join(',', arry);
                    return $"{this.DataColumn.Field} {QueryLogic.IN} ({valStr}) ";
                }
            }

            return string.Empty;
        }

        /// <summary>
        /// Build LessThanOrEqual(<=) Sql
        /// </summary>
        /// <returns>Sql</returns>
        private string BuildLessThanOrEqualSql() => $"{this.DataColumn.Field} {QueryLogic.LessThanOrEqual} {this.Value} ";

        /// <summary>
        /// Build LessThan(<) Sql
        /// </summary>
        /// <returns>Sql</returns>
        private string BuildLessThanSql() => $"{this.DataColumn.Field} {QueryLogic.LessThan} {this.Value} ";

        /// <summary>
        /// Build GreaterThanOrEqual(>=) Sql
        /// </summary>
        /// <returns>Sql</returns>
        private string BuildGreaterThanOrEqualSql() => $"{this.DataColumn.Field} {QueryLogic.GreaterThanOrEqual} {this.Value} ";

        /// <summary>
        /// Build GreaterThan(>) Sql
        /// </summary>
        /// <returns>Sql</returns>
        private string BuildGreaterThanSql() => $"{this.DataColumn.Field} {QueryLogic.GreaterThan} {this.Value} ";

        /// <summary>
        /// Build IsNull Sql
        /// </summary>
        /// <returns>Sql</returns>
        private string BuildIsNullSql() => $"{this.DataColumn.Field} {QueryLogic.IsNull} ";

        /// <summary>
        /// Build IsNotNull Sql
        /// </summary>
        /// <returns>Sql</returns>
        private string BuildIsNotNullSql() => $"{this.DataColumn.Field} {QueryLogic.IsNotNull} ";

        /// <summary>
        /// Build Equal Sql
        /// </summary>
        /// <returns>Sql</returns>
        private string BuildEqualSql() => $"{this.DataColumn.Field} {QueryLogic.Equal} '{this.Value}' ";

        /// <summary>
        /// Build NotEqual Sql
        /// </summary>
        /// <returns>Sql</returns>
        private string BuildNotEqualSql() => $"{this.DataColumn.Field} {QueryLogic.NotEqual} '{this.Value}' ";

        /// <summary>
        /// 生成Between Sql
        /// </summary>
        /// <returns>string</returns>
        private string BuildBetweenSql()
        {
            if (this.Value is IList list && list.Count == 2)
                return $"{this.DataColumn.Field} {QueryLogic.Between} {list[0]} {QueryLogic.AND} {list[^1]} ";
            return string.Empty;
        }

        /// <summary>
        /// 生成Not Between Sql
        /// </summary>
        /// <returns>string</returns>
        private string BuildNotBetweenSql()
        {
            if (this.Value is IList list && list.Count == 2)
                return $"{this.DataColumn.Field} {QueryLogic.NotBetween} {list[0]} {QueryLogic.AND} {list[^1]} ";
            return string.Empty;
        }
    }
}
