using DataLib.Table.Interfaces;

namespace DataLib.Table
{
    /// <summary>
    /// UpdateSetting
    /// </summary>
    public interface IUpdateSetting
    {
        /// <summary>
        /// 合并列时选择的Join 方式
        /// </summary>
        JoinMode Join { get; set; }

        /// <summary>
        /// 数据表
        /// </summary>
        string Table { get; set; }

        /// <summary>
        /// 需要更新数据的表字段集合
        /// </summary>
        IDataColumnCollection UpdateColumns { get; set; }

        /// <summary>
        /// 查询参数
        /// </summary>
        List<Condition> Parameters { get; set; }

        /// <summary>
        /// 需要新增的数据列
        /// </summary>
        IDataColumnCollection NewColumns { get; set; }

        /// <summary>
        /// 需要删除的数据列
        /// </summary>
        IDataColumnCollection DelColumns { get; set; }

        /// <summary>
        /// 主键列
        /// </summary>
        IDataColumnCollection PrimaryColumns { get; set; }

        /// <summary>
        /// 需要新增的数据行
        /// </summary>
        IDataRowCollection Rows { get; set; }

        /// <summary>
        /// Gets or sets the row indexs.
        /// </summary>
        /// <value>
        /// The row indexs.
        /// </value>
        List<int> RowIndexs { get; set; } 
    }
}