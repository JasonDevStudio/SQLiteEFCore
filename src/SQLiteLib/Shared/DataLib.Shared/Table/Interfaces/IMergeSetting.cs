namespace DataLib.Table.Interfaces;

/// <summary>
/// IMergeSetting
/// </summary>
public interface IMergeSetting
{
    /// <summary>
    /// Gets or sets the join.
    /// </summary>
    /// <value>
    /// The join.
    /// </value>
    JoinMode Join { get; set; }

    /// <summary>
    /// Gets or sets the left columns.
    /// </summary>
    /// <value>
    /// The left columns.
    /// </value>
    IDataColumnCollection LeftColumns { get; set; }

    /// <summary>
    /// Gets or sets the left table.
    /// </summary>
    /// <value>
    /// The left table.
    /// </value>
    string LeftTable { get; set; }

    /// <summary>
    /// Gets or sets the macth cloumns.
    /// </summary>
    /// <value>
    /// The macth cloumns.
    /// </value>
    List<(IDataColumn Left, IDataColumn right)> MacthCloumns { get; set; }

    /// <summary>
    /// Creates new columns.
    /// </summary>
    /// <value>
    /// The new columns.
    /// </value>
    IDataColumnCollection NewColumns { get; set; }

    /// <summary>
    /// Gets or sets the right columns.
    /// </summary>
    /// <value>
    /// The right columns.
    /// </value>
    IDataColumnCollection RightColumns { get; set; }

    /// <summary>
    /// Gets or sets the right table.
    /// </summary>
    /// <value>
    /// The right table.
    /// </value>
    string RightTable { get; set; }

    /// <summary>
    /// Gets or sets the table.
    /// </summary>
    /// <value>
    /// The table.
    /// </value>
    string Table { get; set; }
}