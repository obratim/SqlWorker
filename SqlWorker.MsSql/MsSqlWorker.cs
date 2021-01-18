using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;

namespace SqlWorker
{
	/// <summary>
	/// Generator of SqlParameter objects
	/// </summary>
	public class ParametersConstuctorsForMsSql : ADbParameterCreator<SqlParameter>
    {
		/// <summary>
		/// Set parameter size (for types with variable size)
		/// </summary>
		/// <param name="parameter">The parameter</param>
		/// <param name="size">Parameter size</param>
        protected override void SetSize(SqlParameter parameter, int size)
        {
            parameter.Size = size;
        }
    }

    /// <summary>
    /// Adapter for MS Sql Server
    /// </summary>
    public partial class MsSqlWorker
#if NETSTANDARD2_1
    : Async.ASqlWorkerAsync<ParametersConstuctorsForMsSql>
#else
    : ASqlWorker<ParametersConstuctorsForMsSql>
#endif
    {
        private SqlConnection _conn;

        private readonly string _connstr;

        /// <summary>
        /// Database connection
        /// </summary>
        protected override IDbConnection Connection
        {
            get
            {
                if (_conn == null) _conn = new SqlConnection(_connstr);
                return _conn;
            }
        }
        
        /// <summary>
        /// Constructor from connection string
        /// </summary>
        /// <param name="connectionString">The connection string, for example 'Server={0};Database={1};User ID={2};Password={3};Integrated Security=false'</param>
        public MsSqlWorker(string connectionString)
            : base() { _connstr = connectionString; }

        /// <summary>
        /// Constructor for windows authentication
        /// </summary>
        /// <param name="server">The target Sql Server</param>
        /// <param name="dataBase">The target database</param>
        public MsSqlWorker(string server, string dataBase)
            : base()
        {
            _connstr = string.Format("Server={0};Database={1};Integrated Security=true", server, dataBase);
        }

        /// <summary>
        /// Constructor for sql server authentication
        /// </summary>
        /// <param name="server">The target Sql Server</param>
        /// <param name="dataBase">The target database</param>
        /// <param name="login">Username</param>
        /// <param name="password">Password</param>
        public MsSqlWorker(string server, string dataBase, string login, string password)
            : base()
        {
            _connstr = string.Format("Server={0};Database={1};User ID={2};Password={3};Integrated Security=false", server, dataBase, login, password);
        }

        /// <summary>
        /// Constructor from existing sql connection
        /// </summary>
        /// <param name="connection">sql connection</param>
        public MsSqlWorker(SqlConnection connection) : base()
        {
            _conn = connection;
        }

        /// <summary>
        /// Same as TransactionBegin, but returns SqlTransaction object
        /// </summary>
        /// <param name="level"></param>
        /// <returns>The SqlTransaction instance</returns>
        public virtual SqlTransaction SqlTransactionBegin(IsolationLevel level = IsolationLevel.ReadCommitted)
        {
            if (Connection.State != ConnectionState.Open) Connection.Open();
            return _conn.BeginTransaction(level);
        }
        
        /// <summary>
        /// Creates table in database with columns relevant to specified DataTable
        /// </summary>
        /// <param name="source">DataTable, wich structure must be copied</param>
        /// <param name="recreate">Check if targed table exists and drop it if necessary</param>
        virtual public void CreateTableByDataTable(DataTable source, bool recreate = false)
        {
            if (recreate)
            {
                Exec("IF OBJECT_ID(@tname, 'U') IS NOT NULL DROP TABLE " + source.TableName, new SwParameters { { "tname", source.TableName } });
            }

            var columns = new List<DataColumn>();
            columns.AddRange(source.Columns.Cast<DataColumn>());

			Exec(string.Format(@"
CREATE TABLE [{0}] (
    {1}
)
",
			source.TableName,
			string.Join(",\n    ", columns.Select(c => string.Format("[{0}] {1}{4} {2} {3}",
				 c.ColumnName,
				 c.DataType.IsEnum ? "int" : c.DataType == typeof(string) && c.MaxLength < 0 ? "nvarchar(max)" : TypeMapTsql[c.DataType].ToString(),
				 c.AllowDBNull ? "NULL" : "NOT NULL",
				 c.AutoIncrement ? string.Format("identity({0},{1})", c.AutoIncrementSeed, c.AutoIncrementStep) : "",
				 c.MaxLength >= 0 ? string.Format("({0})", c.MaxLength) : ""))
			 )));
        }

        #region Bulk copy

        /// <summary>
        /// Gets new BulkCopy instance with current connection
        /// </summary>
        /// <param name="options">Bulk copy options</param>
        /// <param name="tran">Transaction</param>
        /// <returns>BulkCopy instance</returns>
        protected virtual SqlBulkCopy NewBulkCopyInstance(
            SqlBulkCopyOptions options,
            SqlTransaction tran)
        {
            return new SqlBulkCopy(_conn, options, tran);
        }

        /// <summary>
        /// Default chunk size for bulk copy
        /// </summary>
        public int DefaultChunkSize { get; set; } = 5000;

        /// <summary>
        /// Performs bulk copy from DataTable to specified table
        /// </summary>
        /// <param name="source">Source data</param>
        /// <param name="targetTableName">Target table</param>
        /// <param name="transaction">Transaction</param>
        /// <param name="options">Bulk copy options</param>
        /// <param name="timeout">Timeout</param>
        /// <param name="mappings">Mappings for bulk copy</param>
        /// <param name="createTableIfNotExists">Checks if table exists and creates if necessary</param>
        /// <param name="chunkSize"></param>
        /// <param name="enableStreaming"></param>
        virtual public void BulkCopy(
            DataTable source,
            string targetTableName,
            SqlTransaction transaction,
            SqlBulkCopyOptions options = SqlBulkCopyOptions.Default,
            int? timeout = null,
            IEnumerable<SqlBulkCopyColumnMapping> mappings = null,
            bool createTableIfNotExists = false,
            int? chunkSize = null,
            bool enableStreaming = false)
        {
            if (Connection.State != ConnectionState.Open) Connection.Open();

            if (createTableIfNotExists)
            {
                source.TableName = targetTableName;
                var objId = Query(
                    @"SELECT OBJECT_ID(@name, 'U')",
                    dr => dr.GetNullableInt32(0),
                    new SwParameters { { "name", targetTableName } },
                    timeout,
                    transaction: transaction)
                    .Single();
                if (objId == null)
                    CreateTableByDataTable(source, false);
            }

            using (SqlBulkCopy sbc = NewBulkCopyInstance(options, transaction))
            {
                sbc.DestinationTableName = targetTableName;
                if (mappings == null)
                    foreach (var column in source.Columns)
                        sbc.ColumnMappings.Add(column.ToString(), column.ToString());
				else
					foreach (var m in mappings)
						sbc.ColumnMappings.Add(m);
				sbc.BulkCopyTimeout = timeout ?? DefaultExecutionTimeout;
                sbc.BatchSize = chunkSize ?? DefaultChunkSize;
                sbc.EnableStreaming = enableStreaming;
                sbc.WriteToServer(source);
            }
        }

        /// <summary>
        /// Performs bulk copy from objects collection to target table in database; columns are detected by reflection
        /// </summary>
        /// <typeparam name="T">The generic type of collection</typeparam>
        /// <param name="source">The source collection</param>
        /// <param name="targetTableName">Name of the table, where data will be copied</param>
        /// <param name="transaction">Transaction</param>
        /// <param name="options">Bulk copy options</param>
        /// <param name="chunkSize">If greater then zero, multiple copies will be performed with specified number of rows in each iteration</param>
        /// <param name="timeout">Timeout</param>
        /// <param name="mappings">Mappings for bulk copy</param>
        /// <param name="createTableIfNotExists">Checks if table exists and creates if necessary</param>
        /// <param name="enableStreaming"></param>
		virtual public void BulkCopyWithReflection<T>(
            IEnumerable<T> source,
            string targetTableName,
            SqlTransaction transaction,
            SqlBulkCopyOptions options = SqlBulkCopyOptions.Default,
            int? chunkSize = null,
            int? timeout = null,
            IEnumerable<SqlBulkCopyColumnMapping> mappings = null,
            bool createTableIfNotExists = false,
            bool enableStreaming = false)
        {
            if (Connection.State != ConnectionState.Open) Connection.Open();

            using (SqlBulkCopy sbc = NewBulkCopyInstance(options, transaction))
            using (var srcreader = new EnumerableDbDataReader<T>(source))
            {
                if (createTableIfNotExists)
                {
                    var dataTable = srcreader.GetSchemaTable();

                    dataTable.TableName = targetTableName;
                    var objId = Query(
                        @"SELECT OBJECT_ID(@name, 'U')",
                        dr => dr.GetNullableInt32(0),
                        new SwParameters { { "name", targetTableName } },
                        timeout,
                        transaction: transaction)
                        .Single();
                    if (objId == null)
                        CreateTableByDataTable(dataTable, false);
                }

                sbc.DestinationTableName = targetTableName;
                if (mappings == null)
                    foreach (var column in srcreader.GetSchemaTable().Columns)
                        sbc.ColumnMappings.Add(column.ToString(), column.ToString());
                else
                    foreach (var m in mappings)
                        sbc.ColumnMappings.Add(m);
                sbc.BulkCopyTimeout = timeout ?? DefaultExecutionTimeout;
                sbc.BatchSize = chunkSize ?? DefaultChunkSize;
                sbc.EnableStreaming = enableStreaming;
                sbc.WriteToServer(srcreader);
			}
        }

        #endregion Bulk copy
    }
}