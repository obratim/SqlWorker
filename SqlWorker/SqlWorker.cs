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
    public class SqlWorker : ASqlWorker
    {
        private String _connectionStr;
        private SqlConnection _conn;

        protected override DbConnection Conn
        {
            get
            {
                if (_conn == null) _conn = new SqlConnection(_connectionStr);
                return _conn;
            }
        }

        protected override DbParameter DbParameterConstructor(string paramName, object paramValue) { return new SqlParameter(paramName, paramValue); }

        public SqlWorker(String ConnectionString) { _connectionStr = ConnectionString; }

        #region send files

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

                    InsertValues(tableName, attributes);

                    //bool procNameFlag = !String.IsNullOrEmpty(procName); //check if procname was set
                    //if (!procNameFlag)
                    String procName = "SELECT " + fileDataFieldName + ".PathName() as Path, GET_FILESTREAM_TRANSACTION_CONTEXT as Context from " + tableName + " where " + fileIdFieldName + " = @" + fileIdFieldName;

                    return GetStructFromDB<SqlFileStream>(procName,
                        new SqlParameter(fileIdFieldName, fileId),
                        reader =>
                        {
                            SqlDataReader dr = (SqlDataReader)reader;
                            dr.Read();
                            return new SqlFileStream(dr.GetSqlString(0).Value, dr.GetSqlBinary(1).Value, System.IO.FileAccess.Write);
                        });
                },
                bufLength);
        }

        public int InsertFileGeneric(System.IO.Stream inputStream, FileStreamService InsertDataAndReturnSQLFileStream, long bufLength = 512*1024)
        {
            bool toCloseTranFlag = TransactionIsOpened;
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

        #endregion
    }
}
