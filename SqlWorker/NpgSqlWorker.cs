using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Data.Common;
using Npgsql;

namespace SqlWorker {
    public class NpgSqlWorker : ASqlWorker {
        private String _connectionStr;

        public NpgSqlWorker(String ConnectionString) { _connectionStr = ConnectionString; }

        private NpgsqlConnection _conn;
        protected override DbConnection Conn {
            get {
                if (_conn == null) _conn = new NpgsqlConnection(_connectionStr);
                return _conn;
            }
        }

        protected override DbParameter DbParameterConstructor(string paramName, object paramValue) { return new NpgsqlParameter(paramName, paramValue); }
    }
}
