using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlWorker
{
	public class ParametersConstructorForMySql : ADbParameterCreator<MySql.Data.MySqlClient.MySqlParameter> { }

    public class MySqlWorker : ASqlWorker<ParametersConstructorForMySql>
    {
        private string _connectionString;

        public MySqlWorker(string connectionString, TimeSpan? reconnectPause = null)
            : base(reconnectPause)
        {
            _connectionString = connectionString;
        }

        private MySql.Data.MySqlClient.MySqlConnection _conn;

        protected override IDbConnection Conn
        {
            get
            {
                if (_conn == null) _conn = new MySql.Data.MySqlClient.MySqlConnection(_connectionString);
                return _conn;
            }
        }
    }
}
