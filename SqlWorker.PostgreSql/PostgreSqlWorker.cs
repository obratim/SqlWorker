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
    public class ParametersConstructorForPostgreSql : AbstractDbParameterConstructors
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

    public class PostgreSqlWorker : ASqlWorker<ParametersConstructorForPostgreSql>
    {
        private string _connectionStr;

        public PostgreSqlWorker(string ConnectionString, TimeSpan? reconnectPause = null)
            : base(reconnectPause) { _connectionStr = ConnectionString; }

        private NpgsqlConnection _conn;

        protected override DbConnection Conn
        {
            get
            {
                if (_conn == null) _conn = new NpgsqlConnection(_connectionStr);
                return _conn;
            }
        }
    }
}
