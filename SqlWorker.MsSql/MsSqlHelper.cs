using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;

namespace SqlWorker
{
	/// <summary>
	/// Helpers specific to working with MS SQL Server
	/// </summary>
    public static class MsSqlHelper
    {
		/// <summary>
		/// Converts Dictionary to sequence of <c cref="SqlBulkCopyColumnMapping">SqlBulkCopyColumnMapping</c>, can be used for bulk copy
		/// </summary>
		/// <param name="mappings">Mapping: key - name of property, value - name of DB column</param>
		/// <returns>Sequence of <c cref="SqlBulkCopyColumnMapping">SqlBulkCopyColumnMapping</c>, can be used for bulk copy</returns>
		public static IEnumerable<SqlBulkCopyColumnMapping> ToBulkCopyMappings(this IDictionary<string, string> mappings)
		{
			foreach (var kv in mappings)
				yield return new SqlBulkCopyColumnMapping(kv.Key, kv.Value);
		}
    }
}
