using DataLib.Table.Interfaces;

namespace DataLib.Table
{
    /// <summary>
    /// ICondition
    /// </summary>
    public interface ICondition
    {
        /// <summary>
        /// Gets or sets the binary.
        /// </summary>
        /// <value>
        /// The binary.
        /// </value>
        string Binary { get; set; }

        /// <summary>
        /// Gets or sets the data column.
        /// </summary>
        /// <value>
        /// The data column.
        /// </value>
        IDataColumn DataColumn { get; set; }

        /// <summary>
        /// Gets or sets the logic.
        /// </summary>
        /// <value>
        /// The logic.
        /// </value>
        string Logic { get; set; }

        /// <summary>
        /// Gets or sets the value.
        /// </summary>
        /// <value>
        /// The value.
        /// </value>
        object Value { get; set; }

        /// <summary>
        /// Converts to string.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String" /> that represents this instance.
        /// </returns>
        string ToString();
    }
}