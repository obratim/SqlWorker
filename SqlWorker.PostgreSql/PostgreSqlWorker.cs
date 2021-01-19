using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlWorker
{
	public class ParametersConstructorForPostgreSql : ADbParameterCreator<NpgsqlParameter>
    {
		/// <summary>
		/// Set parameter size (for types with variable size)
		/// </summary>
		/// <param name="parameter">The parameter</param>
		/// <param name="size">Parameter size</param>
        protected override void SetSize(NpgsqlParameter parameter, int size)
        {
            parameter.Size = size;
        }
    }

    public class PostgreSqlWorker
#if NETSTANDARD2_1
    : Async.ASqlWorkerAsync<ParametersConstructorForPostgreSql>
#else
    : ASqlWorker<ParametersConstructorForPostgreSql>
#endif
        , IBulkCopy<PostreSqlBulkCopySettings>
        , IBulkCopyWithReflection<PostreSqlBulkCopySettings>
    {
        private string _connectionStr;

        public PostgreSqlWorker(string ConnectionString)
            : base() { _connectionStr = ConnectionString; }

        private NpgsqlConnection _conn;

        protected override IDbConnection Connection
        {
            get
            {
                if (_conn == null) _conn = new NpgsqlConnection(_connectionStr);
                return _conn;
            }
        }

        public void BulkCopy(DataTable source, string targetTableName, PostreSqlBulkCopySettings bulkCopySettings = null)
        {
            using var dr = source.DataSet.CreateDataReader();
            using var writer = ((Npgsql.NpgsqlConnection)Connection).BeginBinaryImport("");

            dr.PerformBulkCopy(writer, source.Columns);
        }

        public void BulkCopyWithReflection<TItem>(IEnumerable<TItem> source, string targetTableName, PostreSqlBulkCopySettings bulkCopySettings = null)
        {
            using var dr = source.ToDbDataReader();
            using var writer = ((Npgsql.NpgsqlConnection)Connection).BeginBinaryImport("");

            dr.PerformBulkCopy(writer);
        }
    }
}
