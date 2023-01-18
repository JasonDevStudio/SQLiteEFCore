using DataLib.Table.Interfaces;

namespace DataLib.Table.Impl;

/// <summary>
/// MergeSetting
/// </summary>
public class MergeSetting: IMergeSetting
{
    /// <summary>
    /// 合并列时选择的Join 方式
    /// </summary>
    public JoinMode Join { get; set; }

    /// <summary>
    /// 数据表名
    /// </summary>
    public string Table { get; set; }

    /// <summary>
    /// 左侧数据表名
    /// </summary>
    public string LeftTable { get; set; }

    /// <summary>
    /// 右侧数据表名
    /// </summary>
    public string RightTable { get; set; }

    /// <summary>
    /// 需要更新数据的表字段集合
    /// </summary>
    public IDataColumnCollection LeftColumns { get; set; }

    /// <summary>
    /// 需要新增的数据列
    /// </summary>
    public IDataColumnCollection RightColumns { get; set; }

    /// <summary>
    /// 需要新增的数据列
    /// </summary>
    public IDataColumnCollection NewColumns { get; set; }

    /// <summary>
    /// 匹配的列
    /// </summary>
    public List<(IDataColumn Left, IDataColumn right)> MacthCloumns { get; set; }
}