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
    public class ParameterConstuctors_MSSQL : AbstractDbParameterConstructors
    {
        public override DbParameter Create(string paramName, object paramValue, DbType? type = null, ParameterDirection? direction = null)
        {
            if (!type.HasValue) return new SqlParameter(paramName, paramValue);
            var x = new SqlParameter(paramName, type.Value);
            x.Value = paramValue;
            if (direction.HasValue) x.Direction = direction.Value;
            return x;
        }
    }

    public class MSSqlWorker : ASqlWorker<ParameterConstuctors_MSSQL>
    {
        private SqlConnection _conn;

        private String connstr;

        protected override DbConnection Conn
        {
            get
            {
                if (_conn == null) _conn = new SqlConnection(connstr);
                return _conn;
            }
        }

        public MSSqlWorker(String ñonnectionString, TimeSpan? reconnectPause = null)
            : base(reconnectPause) { connstr = ñonnectionString; }

        public MSSqlWorker(String server, String dataBase, TimeSpan? reconnectPause = null)
            : base(reconnectPause)
        {
            connstr = String.Format("Server={0};Database={1};Integrated Security=true", server, dataBase);
        }

        public MSSqlWorker(String server, String dataBase, String login, String password, TimeSpan? reconnectPause = null)
            : base(reconnectPause)
        {
            connstr = String.Format("Server={0};Database={1};User ID={2};Password={3};Integrated Security=false", server, dataBase, login, password);
        }

        #region send files

        public SqlFileStream GetFileStreamFromDB(String tableName, String dataFieldName, System.IO.FileAccess accessType, SqlTransaction transaction, Dictionary<String, Object> attributies, String condition = "", int? timeout = null)
        {
            if (condition == null) condition = "";
            if (string.IsNullOrWhiteSpace(condition))
                condition = attributies.Aggregate<KeyValuePair<String, Object>, String>("", (str, i) => { return str + (String.IsNullOrEmpty(str) ? "" : " and ") + i.Key + " = @" + i.Key; });
            return ManualProcessing("select " + dataFieldName + ".PathName() as Path, GET_FILESTREAM_TRANSACTION_CONTEXT() as Context from " + tableName + " where " + condition,
                dr =>
                {
                    if (!dr.Read()) throw new Exception("No sutch file");
                    return new SqlFileStream(dr.GetString(0), (byte[])dr[1], accessType);
                }, attributies, timeout, transaction: transaction);
        }

        public delegate SqlFileStream FileStreamService();

        public int InsertFileNoStoredProcs(
            String tableName,
            String fileIdFieldName, String fileDataFieldName,
            SqlTransaction transaction,
            Dictionary<String, Object> attributes,
            System.IO.Stream inputStream,
            long bufLength = 512*1024,
            int? timeout = null
            //, String procName = null, int procFilePathIndex = 0, int procFileTokenIndex = 1
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

                    return GetFileStreamFromDB(tableName, fileDataFieldName, System.IO.FileAccess.Write, transaction, new Dictionary<string, object>() { { fileIdFieldName, attributes[fileIdFieldName] } }, timeout: timeout);
                },
                bufLength);
        }

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

        virtual public bool CreateTableByDataTable(DataTable source, bool recreate = false)
        {
            if (recreate)
            {
                ExecuteNonQuery("IF OBJECT_ID(@tname, 'U') IS NOT NULL DROP TABLE " + source.TableName, new SWParameters { { "tname", source.TableName } });
            }

            var columns = new List<DataColumn> { };
            foreach (DataColumn c in source.Columns)
                columns.Add(c);

            ExecuteNonQuery(String.Format(@"
CREATE TABLE {0} (
    {1}
)
", source.TableName, String.Join(",\n    ", columns.Select(c => String.Format("{0} {1}{4} {2} {3}",
         c.ColumnName, typeMap_TSQL[c.DataType].ToString(),
         c.AllowDBNull ? "NULL" : "NOT NULL",
         c.AutoIncrement ? String.Format("identity({0},{1})", c.AutoIncrementSeed, c.AutoIncrementStep) : "",
         c.MaxLength > 0 ? String.Format("({0})", c.MaxLength) : ""))
     )));

            /***************************************
            ExecuteNonQuery(String.Format(@"
            if exists (select * from sysobjects where name='{0}' and xtype='U')
            begin
                drop table {0}
            end
            go
            CREATE TABLE {0} (
            {1}
            );
            ", source.TableName, String.Join(",\n\t", (from c in source.Columns.Cast<DataColumn>() select c.ColumnName + " " + typeMap[c.DataType].ToString() + (c.AllowDBNull ? " NULL" : " NOT NULL")))));

            ***************************************/
            return true;
        }

        #region Bulk copy
        /// <summary>
        /// Performs bulk copy from DataTable to specified table
        /// </summary>
        /// <param name="source"></param>
        /// <param name="targetTableName"></param>
        /// <param name="transaction"></param>
        /// <param name="options"></param>
        /// <param name="timeout"></param>
        /// <param name="mappings"></param>
        virtual public void BulkCopy(
            DataTable source,
            String targetTableName,
            SqlTransaction transaction,
            SqlBulkCopyOptions options = SqlBulkCopyOptions.Default,
            int? timeout = null,
            SqlBulkCopyColumnMappingCollection mappings = null
            )
        {
            if (Conn.State != ConnectionState.Open) Conn.Open();

            using (SqlBulkCopy sbc = new SqlBulkCopy(_conn, options, transaction))
            {
                sbc.DestinationTableName = targetTableName;
                if (mappings == null)
                    foreach (var column in source.Columns)
                        sbc.ColumnMappings.Add(column.ToString(), column.ToString());
                sbc.BulkCopyTimeout = timeout ?? DefaultExecutionTimeout;
                sbc.WriteToServer(source);
            }
        }

        /// <summary>
        /// Performs bulk copy from multiple DataTable objects to specified table. Each DataTable will be disposed!
        /// </summary>
        /// <param name="source">IEnumerable with datatables. Each datatable will be disposed</param>
        /// <param name="targetTableName"></param>
        /// <param name="transaction"></param>
        /// <param name="options"></param>
        /// <param name="timeout"></param>
        /// <param name="mappings"></param>
        virtual public void BulkCopy(
            IEnumerable<DataTable> source,
            String targetTableName,
            SqlTransaction transaction,
            SqlBulkCopyOptions options = SqlBulkCopyOptions.Default,
            int? timeout = null,
            SqlBulkCopyColumnMappingCollection mappings = null
            )
        {
            if (Conn.State != ConnectionState.Open) Conn.Open();

            using (SqlBulkCopy sbc = new SqlBulkCopy(_conn, options, transaction))
            {
                        sbc.DestinationTableName = targetTableName;
                        sbc.BulkCopyTimeout = timeout ?? DefaultExecutionTimeout;

                using (var enumerator = source.GetEnumerator())
                {
                    if (!enumerator.MoveNext()) return;

                    using (enumerator.Current)
                    {
                        if (mappings == null)
                            foreach (var column in enumerator.Current.Columns)
                                sbc.ColumnMappings.Add(column.ToString(), column.ToString());

                        sbc.WriteToServer(enumerator.Current);
                    }
                    while (enumerator.MoveNext())
                        using (enumerator.Current)
                        {
                            sbc.WriteToServer(enumerator.Current);
                        }
                } // enumerator
            } // bulk coupy
        } // func

        /// <summary>
        /// Performs bulk copy from objects collection to target table in database; columns are detected by reflection
        /// </summary>
        /// <typeparam name="T">The generic type of collection</typeparam>
        /// <param name="source">The source collection</param>
        /// <param name="targetTableName">Name of the table, where data will be copied</param>
        /// <param name="transaction"></param>
        /// <param name="options">Bulk copy options</param>
        /// <param name="chunkSize">If greater then zero, multiple copies will be performed with specified number of rows in each iteration</param>
        /// <param name="timeout"></param>
        /// <param name="mappings"></param>
		virtual public void BulkCopyWithReflection<T>(
            IEnumerable<T> source,
            String targetTableName,
            SqlTransaction transaction,
            SqlBulkCopyOptions options = SqlBulkCopyOptions.Default,
            int chunkSize = 0,
            int? timeout = null,
            SqlBulkCopyColumnMappingCollection mappings = null
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