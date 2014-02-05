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

        public void InsertFile(
            String tableName,
            String fileIdFieldName, String fileDataFieldName,
            Dictionary<String, Object> attributes,
            System.IO.Stream inputStream,
            long inputLength,
            long bufLength = 512*1024,
            String procName = null, int procFilePathIndex = 0, int procFileTokenIndex = 1)
        {
            Guid fileId;
            if (!attributes.ContainsKey(fileIdFieldName))
            {
                fileId = Guid.NewGuid();
                attributes.Add(fileIdFieldName, fileId);
            }
            else fileId = (Guid)attributes[fileIdFieldName];

            InsertValues(tableName, attributes);

            SqlFileStream sfs = null;

            bool procNameFlag = !String.IsNullOrEmpty(procName); //check if procname was set
            if (!procNameFlag) procName = "SELECT " + fileDataFieldName + ".PathName() as Path, GET_FILESTREAM_TRANSACTION_CONTEXT as Context from " + tableName + " where " + fileIdFieldName + " = @" + fileIdFieldName;
            GetStructFromDB<bool>(procName,
                new SqlParameter(fileIdFieldName, fileId),
                reader =>
                {
                    SqlDataReader dr = (SqlDataReader)reader;
                    dr.Read();
                    sfs = new SqlFileStream(dr.GetSqlString(0 + (procNameFlag ? procFilePathIndex : 0)).Value, dr.GetSqlBinary(0 + (procNameFlag ? procFilePathIndex : 1)).Value, System.IO.FileAccess.Write);
                    return true;
                });

            byte[] buffer = new byte[bufLength];
            int readen = inputStream.Read(buffer, 0, buffer.Length);
            while (readen > 0)
            {
                sfs.Write(buffer, 0, readen);
                readen = inputStream.Read(buffer, 0, buffer.Length);
            }
            sfs.Close();
        }

        #endregion
    }
}
