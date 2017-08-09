using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlWorker
{
    public class ParametersConstructorForMySql : AbstractDbParameterConstructors
    {
        public override DbParameter Create(string name, object value, DbType? type = default(DbType?), ParameterDirection? direction = default(ParameterDirection?))
        {
            if (!type.HasValue && !direction.HasValue) return new MySql.Data.MySqlClient.MySqlParameter(name, value);

            var parameter = type.HasValue ? new MySql.Data.MySqlClient.MySqlParameter(name, type.Value) { Value = value } : new MySql.Data.MySqlClient.MySqlParameter(name, value);
            parameter.Direction = direction ?? ParameterDirection.Input;
            return parameter;
        }
    }

    public class MySqlWorker : ASqlWorker<ParametersConstructorForMySql>
    {
        private string _connectionString;

        public MySqlWorker(string connectionString, TimeSpan? reconnectPause = null)
            : base(reconnectPause)
        {
            _connectionString = connectionString;
        }

        private MySql.Data.MySqlClient.MySqlConnection _conn;

        protected override DbConnection Conn
        {
            get
            {
                if (_conn == null) _conn = new MySql.Data.MySqlClient.MySqlConnection(_connectionString);
                return _conn;
            }
        }
    }
}
