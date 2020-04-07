using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;

namespace SqlWorker
{
    public static class MsSqlHelper
    {
		public static IEnumerable<SqlBulkCopyColumnMapping> ToBulkCopyMappings(this IDictionary<string, string> mappings)
		{
			foreach (var kv in mappings)
				yield return new SqlBulkCopyColumnMapping(kv.Key, kv.Value);
		}
    }
}
