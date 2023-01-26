using DataLib.Table.Impl;
using DataLib.Table.Interfaces;

namespace DataLib.Table;

public class QuerySetting : IQuerySetting
{
    /// <summary>
    /// Gets or sets the columns.
    /// </summary>
    /// <value>
    /// The columns.
    /// </value>
    public IDataColumnCollection Columns { get; set; } = new DataColumnCollection();

    /// <summary>
    /// Gets or sets the order fields.
    /// </summary>
    /// <value>
    /// The order fields.
    /// </value>
    public IDataColumnCollection OrderFields { get; set; } = new DataColumnCollection();

    /// <summary>
    /// Gets or sets the parameters.
    /// </summary>
    /// <value>
    /// The parameters.
    /// </value>
    public List<IQueryFilter> Parameters { get; set; } = new();

    /// <summary>
    /// Gets or sets the table.
    /// </summary>
    /// <value>
    /// The table.
    /// </value>
    [Newtonsoft.Json.JsonIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    public IDataTable Table { get; set; }
}