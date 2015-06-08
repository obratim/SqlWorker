using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Data.Common;
using System.Data.SqlTypes;

namespace SqlWorker
{
    public class MSSQLParameterConstuctors : AbstractDbParameterConstructors
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

    public class MSSqlWorker : ASqlWorker<MSSQLParameterConstuctors>
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

        public MSSqlWorker(String ConnectionString, TimeSpan? reconnectPause = null)
            : base(reconnectPause)
        { connstr = ConnectionString; }

        public MSSqlWorker(String Server, String DataBase, TimeSpan? reconnectPause = null)
            : base(reconnectPause)
        {
            connstr = String.Format("Server={0};Database={1};Integrated Security=true", Server, DataBase);
        }

        public MSSqlWorker(String Server, String DataBase, String Login, String Password, TimeSpan? reconnectPause = null)
            : base(reconnectPause)
        {
            connstr = String.Format("Server={0};Database={1};User ID={2};Password={3};Integrated Security=false", Server, DataBase, Login, Password);
        }

        #region send files

        public SqlFileStream GetFileStreamFromDB(String tableName, String dataFieldName, System.IO.FileAccess accessType, Dictionary<String, Object> attributies, String condition = "")
        {
            if (!TransactionIsOpened) throw new Exception("Must perform file operations in transaction!");
            if (condition == null) condition = "";
            if (string.IsNullOrWhiteSpace(condition))
                condition = attributies.Aggregate<KeyValuePair<String, Object>, String>("", (str, i) => { return str + (String.IsNullOrEmpty(str) ? "" : " and ") + i.Key + " = @" + i.Key; });
            return ManualProcessing("select " + dataFieldName + ".PathName() as Path, GET_FILESTREAM_TRANSACTION_CONTEXT() as Context from " + tableName + " where " + condition,
                dr =>
                {
                    if (!dr.Read()) throw new Exception("No sutch file");
                    return new SqlFileStream(dr.GetString(0), (byte[])dr[1], accessType);
                }, attributies);
        }

        public delegate SqlFileStream FileStreamService();

        public int InsertFileNoStoredProcs(
            String tableName,
            String fileIdFieldName, String fileDataFieldName,
            Dictionary<String, Object> attributes,
            System.IO.Stream inputStream,
            long bufLength = 512*1024
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

                    return GetFileStreamFromDB(tableName, fileDataFieldName, System.IO.FileAccess.Write, new Dictionary<string, object>() { { fileIdFieldName, attributes[fileIdFieldName] } });
                },
                bufLength);
        }

        public int InsertFileGeneric(System.IO.Stream inputStream, FileStreamService InsertDataAndReturnSQLFileStream, long bufLength = 512*1024)
        {
            try
            {
                bool toCloseTranFlag = !TransactionIsOpened;
                if (!TransactionIsOpened) TransactionBegin();

                var sfs = InsertDataAndReturnSQLFileStream();

                byte[] buffer = new byte[bufLength];
                int readen = inputStream.Read(buffer, 0, buffer.Length);
                int writen = readen;
                while (readen > 0)
                {
                    sfs.Write(buffer, 0, readen);
                    readen = inputStream.Read(buffer, 0, buffer.Length);
                    writen += readen;
                }
                sfs.Close();

                if (toCloseTranFlag) TransactionCommit();
                return writen;
            }
            catch
            {
                if (TransactionIsOpened) try { TransactionRollback(); }
                    catch { }
                return -1;
            }
        }

        #endregion
    }
}
