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
	/// <summary>
	/// Generator of SqlParameter objects
	/// </summary>
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

    /// <summary>
    /// Adapter for PostgreSQL
    /// </summary>
    public class PostgreSqlWorker
#if NETSTANDARD2_1
    : Async.ASqlWorkerAsync<ParametersConstructorForPostgreSql>
#else
    : ASqlWorker<ParametersConstructorForPostgreSql>
#endif
        , IBulkCopy<PostgreSqlBulkCopySettings>
        , IBulkCopyWithReflection<PostgreSqlBulkCopySettings>
    {
        private readonly string _connectionStr;

        /// <summary>
        /// Constructor from connection string
        /// </summary>
        /// <param name="connectionString">The connection string, for example "Host={0};Database={1};User ID={2};Password={3};"</param>
        public PostgreSqlWorker(string connectionString)
            : base()
        {
            _connectionStr = connectionString;
        }

        /// <summary>
        /// Constructor for sql server authentication
        /// </summary>
        /// <param name="host">The target Sql Server</param>
        /// <param name="database">The target database</param>
        /// <param name="user">Username</param>
        /// <param name="password">Password</param>
        public PostgreSqlWorker(string host, string database, string user, string password)
            : base()
        {
            _connectionStr = $"Host={host};Database={database};User ID={user};Password={password};";
        }

        private NpgsqlConnection _conn;

        /// <summary>
        /// Database connection
        /// </summary>
        protected override IDbConnection Connection
        {
            get
            {
                if (_conn == null) _conn = new NpgsqlConnection(_connectionStr);
                return _conn;
            }
        }

        /// <summary>
        /// Performs bulk copy from DataTable to specified table
        /// </summary>
        /// <param name="source">Source data</param>
        /// <param name="targetTableName">Target table</param>
        /// <param name="bulkCopySettings">Bulk copy options</param>
        public void BulkCopy(DataTable source, string targetTableName, PostgreSqlBulkCopySettings bulkCopySettings = null)
        {
            using var dr = source.DataSet.CreateDataReader();
            if (Connection.State != ConnectionState.Open) Connection.Open();
            using var writer = ((Npgsql.NpgsqlConnection)Connection).BeginBinaryImport(source.Columns.BulkCopyCommand(targetTableName));

            dr.PerformBulkCopy(writer, source.Columns, bulkCopySettings);
        }

        /// <summary>
        /// Performs bulk copy from objects sequence to target table in database; columns are detected by reflection
        /// </summary>
        /// <typeparam name="T">The generic type of collection</typeparam>
        /// <param name="source">The source collection</param>
        /// <param name="targetTableName">Name of the table, where data will be copied</param>
        /// <param name="bulkCopySettings">Bulk copy options</param>
        public void BulkCopy<T>(IEnumerable<T> source, string targetTableName, PostgreSqlBulkCopySettings bulkCopySettings = null)
        {
            if (Connection.State != ConnectionState.Open) Connection.Open();
            using var writer = ((Npgsql.NpgsqlConnection)Connection).BeginBinaryImport(BulkCopyGeneric<T>.BulkCopyCommand(targetTableName));

            BulkCopyGeneric<T>.BulkCopy(source, writer, bulkCopySettings);
        }
    }
}
