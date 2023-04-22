using DataLib.Table.Interfaces;

namespace DataLib.Table.Impl;

/// <summary>
/// MergeSetting
/// </summary>
public class MergeSetting : IMergeSetting
{
    /// <summary>
    /// 合并列时选择的Join 方式
    /// </summary>
    public JoinMode Join { get; set; } = JoinMode.INNER_JOIN;

    /// <summary>
    /// 数据表名
    /// </summary>
    public string TableId { get; set; }

    /// <summary>
    /// Gets or sets the name of the table.
    /// </summary>
    /// <value>
    /// The name of the table.
    /// </value>
    public string TableName { get; set; }

    /// <summary>
    /// 右侧数据表名
    /// </summary>
    public string RightTableId { get; set; }

    /// <summary>
    /// 右侧数据表名
    /// </summary>
    public string RightTableName { get; set; }

    /// <summary>
    /// 需要更新数据的表字段集合
    /// </summary>
    public IDataColumnCollection LeftColumns { get; set; } = new DataColumnCollection();

    /// <summary>
    /// 需要新增的数据列
    /// </summary>
    public IDataColumnCollection RightColumns { get; set; } = new DataColumnCollection();

    /// <summary>
    /// 需要新增的数据列
    /// </summary>
    public IDataColumnCollection NewColumns { get; set; } = new DataColumnCollection();

    /// <summary>
    /// 匹配的列
    /// </summary>
    public List<(IDataColumn Left, IDataColumn right)> MacthCloumns { get; set; } = new();
    
    /// <summary>
    /// 左侧表
    /// </summary>
    [Newtonsoft.Json.JsonIgnore] 
    public IDataTable Table { get; set; }
    
    /// <summary>
    /// 右侧表
    /// </summary>
    [Newtonsoft.Json.JsonIgnore] 
    public IDataTable RightTable { get; set; }
}