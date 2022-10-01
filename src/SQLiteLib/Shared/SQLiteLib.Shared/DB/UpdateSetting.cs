using SQLiteLib.Table.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;

namespace SQLiteEFCore.Shared.DB
{
    /// <summary>
    /// UpdateSetting
    /// </summary>
    public class UpdateSetting
    {
        /// <summary>
        /// 数据表
        /// </summary>
        [Newtonsoft.Json.JsonIgnore]
        public IDataTable Table { get; set; }

        /// <summary>
        /// 需要更新数据的表字段集合
        /// </summary>
        public IDataColumnCollection UpdateColumns { get; set; }

        /// <summary>
        /// 查询参数
        /// </summary>
        public List<Condition> Parameters { get; set; }

        /// <summary>
        /// 需要新增的数据列
        /// </summary>
        public IDataColumnCollection AddColumns { get; set; }

        /// <summary>
        /// 主键列
        /// </summary>
        public IDataColumnCollection PrimaryColumns { get; set; }

        /// <summary>
        /// 需要新增的数据行
        /// </summary>
        public IDataRowCollection Rows { get; set; }
    }
}
