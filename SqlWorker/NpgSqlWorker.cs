using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Data.Common;
using Npgsql;

namespace SqlWorker {
    public class NpgParameterConstructor : AbstractDbParameterConstructors
    {
        public override DbParameter Create(string name, object value, DbType? type = null, ParameterDirection? direction = null)
        {
            if (!type.HasValue) return new NpgsqlParameter(name, value);

            var x = new NpgsqlParameter(name, type.Value);
            x.Value = value;
            if (direction.HasValue) x.Direction = direction.Value;
            return x;
        }
    }

    public class NpgSqlWorker : ASqlWorker<NpgParameterConstructor> {
        private String _connectionStr;

        public NpgSqlWorker(String ConnectionString, TimeSpan? reconnectPause = null)
            : base(reconnectPause)
        { _connectionStr = ConnectionString; }

        private NpgsqlConnection _conn;
        protected override DbConnection Conn {
            get {
                if (_conn == null) _conn = new NpgsqlConnection(_connectionStr);
                return _conn;
            }
        }

        public override void Dispose(bool commit)
        {
            if (!commit && TransactionIsOpened) TransactionRollback();
            if (Conn.State != ConnectionState.Closed) _conn.Close();
            _conn.Dispose();
        }
    }
}
