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
	public class ParametersConstuctorsForMsSql : ADbParameterCreator<SqlParameter> { }

    /// <summary>
    /// Adapter for MS Sql Server
    /// </summary>
    public class MsSqlWorker : ASqlWorker<ParametersConstuctorsForMsSql>
    {
        private SqlConnection _conn;

        private readonly string _connstr;

        /// <summary>
        /// Database connection
        /// </summary>
        protected override IDbConnection Conn
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
        /// <param name="connectionString">The connection string</param>
        /// <param name="reconnectPause">Period for 'ReOpenConnection' method</param>
        public MsSqlWorker(string connectionString, TimeSpan? reconnectPause = null)
            : base(reconnectPause) { _connstr = connectionString; }

        /// <summary>
        /// Constructor for windows authentication
        /// </summary>
        /// <param name="server">The target Sql Server</param>
        /// <param name="dataBase">The target database</param>
        /// <param name="reconnectPause">Period for 'ReOpenConnection' method</param>
        public MsSqlWorker(string server, string dataBase, TimeSpan? reconnectPause = null)
            : base(reconnectPause)
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
        /// <param name="reconnectPause">Period for 'ReOpenConnection' method</param>
        public MsSqlWorker(string server, string dataBase, string login, string password, TimeSpan? reconnectPause = null)
            : base(reconnectPause)
        {
            _connstr = string.Format("Server={0};Database={1};User ID={2};Password={3};Integrated Security=false", server, dataBase, login, password);
        }

        /// <summary>
        /// Same as TransactionBegin, but returns SqlTransaction object
        /// </summary>
        /// <param name="level"></param>
        /// <returns>The SqlTransaction instance</returns>
        public virtual SqlTransaction SqlTransactionBegin(IsolationLevel level = IsolationLevel.ReadCommitted)
        {
            if (Conn.State != ConnectionState.Open) Conn.Open();
            return _conn.BeginTransaction(level);
        }

        #region send files

        /// <summary>
        /// Gets stream from filestream column
        /// </summary>
        /// <param name="tableName">The target table</param>
        /// <param name="dataFieldName">FileStream column</param>
        /// <param name="accessType">Access type</param>
        /// <param name="transaction">Transaction must be opened</param>
        /// <param name="attributies">Query parameters</param>
        /// <param name="condition">Condition for row selection</param>
        /// <param name="timeout">Timeout</param>
        /// <returns>The SqlFileStream instance</returns>
        public SqlFileStream GetFileStreamFromDB(string tableName, string dataFieldName, System.IO.FileAccess accessType, SqlTransaction transaction, Dictionary<string, object> attributies, string condition = "", int? timeout = null)
        {
            if (condition == null) condition = "";
            if (string.IsNullOrWhiteSpace(condition))
                condition = attributies.Aggregate("", (str, i) => str + (string.IsNullOrEmpty(str) ? "" : " and ") + i.Key + " = @" + i.Key);
            return ManualProcessing("select " + dataFieldName + ".PathName() as Path, GET_FILESTREAM_TRANSACTION_CONTEXT() as Context from " + tableName + " where " + condition,
                dr =>
                {
                    if (!dr.Read()) throw new Exception("No sutch file");
                    return new SqlFileStream(dr.GetString(0), (byte[])dr[1], accessType);
                }, attributies, timeout, transaction: transaction);
        }

        /// <summary>
        /// Delegate for obtaining SqlFileStream
        /// </summary>
        /// <returns>SqlFileStream object</returns>
        public delegate SqlFileStream FileStreamService();

        /// <summary>
        /// Performs insertion data into table with FileStream column
        /// </summary>
        /// <param name="tableName">The target table</param>
        /// <param name="fileIdFieldName">Name of FielId column</param>
        /// <param name="fileDataFieldName">Name of FileStream column</param>
        /// <param name="transaction">Transaction must be opened</param>
        /// <param name="attributes">Other row values to insert</param>
        /// <param name="inputStream">Source stream</param>
        /// <param name="bufLength">Buffer length</param>
        /// <param name="timeout">Timeout</param>
        /// <returns>Written size</returns>
        public int InsertFileNoStoredProcs(
            string tableName,
            string fileIdFieldName, string fileDataFieldName,
            SqlTransaction transaction,
            Dictionary<string, object> attributes,
            System.IO.Stream inputStream,
            long bufLength = 512*1024,
            int? timeout = null
            //, string procName = null, int procFilePathIndex = 0, int procFileTokenIndex = 1
        )
        {
            return InsertFileGeneric(inputStream,
                () =>
                {
                    Guid fileId;
                    if (!attributes.ContainsKey(fileIdFieldName))
                    {
                        fileId = Guid.NewGuid();
                        attributes.Add(fileIdFieldName, fileId);
                    }
                    else fileId = (Guid)attributes[fileIdFieldName];
                    attributes[fileDataFieldName] = new byte[0];

                    InsertValues(tableName, attributes);

                    return GetFileStreamFromDB(tableName, fileDataFieldName, System.IO.FileAccess.Write, transaction, new Dictionary<string, object> { { fileIdFieldName, attributes[fileIdFieldName] } }, timeout: timeout);
                },
                bufLength);
        }

        /// <summary>
        /// Performs writing data into table with FileStream column
        /// </summary>
        /// <param name="inputStream">Source stream</param>
        /// <param name="insertDataAndReturnSqlFileStream">Delegate for obtaining SqlFileStream</param>
        /// <param name="bufLength">Buffer length</param>
        /// <returns>Written size</returns>
        public int InsertFileGeneric(System.IO.Stream inputStream, FileStreamService insertDataAndReturnSqlFileStream, long bufLength = 512 * 1024)
        {
            int writen = 0;
            using (var sfs = insertDataAndReturnSqlFileStream())
            {

                byte[] buffer = new byte[bufLength];
                int readen = inputStream.Read(buffer, 0, buffer.Length);
                writen = readen;
                while (readen > 0)
                {
                    sfs.Write(buffer, 0, readen);
                    readen = inputStream.Read(buffer, 0, buffer.Length);
                    writen += readen;
                }
            }
            return writen;
        }

        #endregion send files

        /// <summary>
        /// Creates table in database with columns relevant to specified DataTable
        /// </summary>
        /// <param name="source">DataTable, wich structure must be copied</param>
        /// <param name="recreate">Check if targed table exists and drop it if necessary</param>
        virtual public void CreateTableByDataTable(DataTable source, bool recreate = false)
        {
            if (recreate)
            {
                Exec("IF OBJECT_ID(@tname, 'U') IS NOT NULL DROP TABLE " + source.TableName, new SWParameters { { "tname", source.TableName } });
            }

            var columns = new List<DataColumn>();
            columns.AddRange(source.Columns.Cast<DataColumn>());

			Exec(string.Format(@"
CREATE TABLE {0} (
    {1}
)
",
			source.TableName,
			string.Join(",\n    ", columns.Select(c => string.Format("[{0}] {1}{4} {2} {3}",
				 c.ColumnName,
				 TypeMapTsql[c.DataType].ToString(),
				 c.AllowDBNull ? "NULL" : "NOT NULL",
				 c.AutoIncrement ? string.Format("identity({0},{1})", c.AutoIncrementSeed, c.AutoIncrementStep) : "",
				 c.MaxLength > 0 ? string.Format("({0})", c.MaxLength) : ""))
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
        /// Performs bulk copy from DataTable to specified table
        /// </summary>
        /// <param name="source">Source data</param>
        /// <param name="targetTableName">Target table</param>
        /// <param name="transaction">Transaction</param>
        /// <param name="options">Bulk copy options</param>
        /// <param name="timeout">Timeout</param>
        /// <param name="mappings">Mappings for bulk copy</param>
        virtual public void BulkCopy(
            DataTable source,
            string targetTableName,
            SqlTransaction transaction,
            SqlBulkCopyOptions options = SqlBulkCopyOptions.Default,
            int? timeout = null,
            IEnumerable<SqlBulkCopyColumnMapping> mappings = null
            )
        {
            if (Conn.State != ConnectionState.Open) Conn.Open();

            using (SqlBulkCopy sbc = NewBulkCopyInstance(options, transaction))
            {
                sbc.DestinationTableName = targetTableName;
                if (mappings == null)
                    foreach (var column in source.Columns)
                        sbc.ColumnMappings.Add(column.ToString(), column.ToString());
				else
					foreach (SqlBulkCopyColumnMapping m in mappings)
						sbc.ColumnMappings.Add(m);
				sbc.BulkCopyTimeout = timeout ?? DefaultExecutionTimeout;
                sbc.WriteToServer(source);
            }
        }

        /// <summary>
        /// Performs bulk copy from multiple DataTable objects to specified table. Each DataTable will be disposed!
        /// </summary>
        /// <param name="source">IEnumerable with datatables. Each datatable will be disposed</param>
        /// <param name="targetTableName">Target table</param>
        /// <param name="transaction">Transaction</param>
        /// <param name="options">Bulk copy options</param>
        /// <param name="timeout">Timeout</param>
        /// <param name="mappings">Mappings for bulk copy</param>
        virtual public void BulkCopy(
            IEnumerable<DataTable> source,
            string targetTableName,
            SqlTransaction transaction,
            SqlBulkCopyOptions options = SqlBulkCopyOptions.Default,
            int? timeout = null,
            IEnumerable<SqlBulkCopyColumnMapping> mappings = null
            )
        {
            if (Conn.State != ConnectionState.Open) Conn.Open();

            using (SqlBulkCopy sbc = NewBulkCopyInstance(options, transaction))
            {
                sbc.DestinationTableName = targetTableName;
                sbc.BulkCopyTimeout = timeout ?? DefaultExecutionTimeout;

                using (var enumerator = source.GetEnumerator())
                {
                    if (!enumerator.MoveNext()) return;

                    try
                    {
                        if (mappings == null)
                        {
                            foreach (var column in enumerator.Current.Columns)
                                sbc.ColumnMappings.Add(column.ToString(), column.ToString());
                        }
                        else foreach (SqlBulkCopyColumnMapping m in mappings)
                                sbc.ColumnMappings.Add(m);

                        sbc.WriteToServer(enumerator.Current);

                        while (enumerator.MoveNext())
                            using (enumerator.Current)
                            {
                                sbc.WriteToServer(enumerator.Current);
                            }
                    }
                    finally { enumerator.Current.Dispose(); }
                } // enumerator
            } // bulk coupy
        } // func

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
		virtual public void BulkCopyWithReflection<T>(
            IEnumerable<T> source,
            string targetTableName,
            SqlTransaction transaction,
            SqlBulkCopyOptions options = SqlBulkCopyOptions.Default,
            int chunkSize = 0,
            int? timeout = null,
            IEnumerable<SqlBulkCopyColumnMapping> mappings = null
            )
		{
		    if (chunkSize <= 0)
		    {
		        using (var dt = source.AsDataTable())
		        {
		            BulkCopy(dt, targetTableName, transaction, options, timeout, mappings);
		        }
		    }
		    else
            {
                BulkCopy(source.AsDataTable(chunkSize), targetTableName, transaction, options, timeout, mappings);
            }
		}
        
        #endregion Bulk copy
    }
}