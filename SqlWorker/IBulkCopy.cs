using System.Collections.Generic;
using System.Data;

namespace SqlWorker
{
    /// <summary>
    /// Bulk copy settings may vary for different implementations
    /// </summary>
    public interface IBulkCopySettings {}

    /// <summary>
    /// Implementers can perform bulk copy with a DataTable
    /// </summary>
    /// <typeparam name="T">Represents specific bulk copy settings</typeparam>
    public interface IBulkCopy<T>
        where T : IBulkCopySettings
    {
        /// <summary>
        /// Performs bulk copy from DataTable to specified table
        /// </summary>
        /// <param name="source">Source data</param>
        /// <param name="targetTableName">Target table</param>
        /// <param name="bulkCopySettings">Settings for this implementation of bulk copy</param>
        void BulkCopy(
            DataTable source,
            string targetTableName,
            T bulkCopySettings = default(T));
    }

    /// <summary>
    /// Implementers can perform bulk copy with generic enumeration
    /// </summary>
    /// <typeparam name="T">Represents specific bulk copy settings</typeparam>
    public interface IBulkCopyWithReflection<T>
        where T : IBulkCopySettings
    {
        /// <summary>
        /// Performs bulk copy from objects collection to target table in database; columns are detected by reflection
        /// </summary>
        /// <typeparam name="TItem">The generic type of collection</typeparam>
        /// <param name="source">The source collection</param>
        /// <param name="targetTableName">Name of the table, where data will be copied</param>
        /// <param name="bulkCopySettings">Settings for this implementation of bulk copy</param>
		void BulkCopy<TItem>(
            IEnumerable<TItem> source,
            string targetTableName,
            T bulkCopySettings = default(T));
    }
}
