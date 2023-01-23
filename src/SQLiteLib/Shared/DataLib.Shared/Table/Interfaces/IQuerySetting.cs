using DataLib.Table.Interfaces;

namespace DataLib.Table
{
    public interface IQuerySetting
    {
        /// <summary>
        /// Gets or sets the columns.
        /// </summary>
        /// <value>
        /// The columns.
        /// </value>
        IDataColumnCollection Columns { get; set; }

        /// <summary>
        /// Gets or sets the order fields.
        /// </summary>
        /// <value>
        /// The order fields.
        /// </value>
        IDataColumnCollection OrderFields { get; set; }

        /// <summary>
        /// Gets or sets the parameters.
        /// </summary>
        /// <value>
        /// The parameters.
        /// </value>
        List<IQueryFilter> Parameters { get; set; }

        /// <summary>
        /// Gets or sets the table.
        /// </summary>
        /// <value>
        /// The table.
        /// </value>
        IDataTable Table { get; set; }
    }
}