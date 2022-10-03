using SQLiteLib.Table.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;

namespace SQLiteLib
{
    /// <summary>
    /// UpdateSetting
    /// </summary>
    public class UpdateSetting
    {
        /// <summary>
        /// 合并列时选择的Join 方式
        /// </summary>
        public JoinMode Join { get; set; }

        /// <summary>
        /// 数据表
        /// </summary> 
        public string Table { get; set; }

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
        /// 需要删除的数据列
        /// </summary>
        public IDataColumnCollection DelColumns { get; set; }

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
