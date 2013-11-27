using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Data.Common;

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
                if (_conn == null) _conn = new SqlConnection();
                return _conn;
            }
        }
        public SqlWorker(String ConnectionString) { _connectionStr = ConnectionString; }
    }
}
