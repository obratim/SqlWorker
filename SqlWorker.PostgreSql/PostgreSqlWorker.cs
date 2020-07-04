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
	public class ParametersConstructorForPostgreSql : ADbParameterCreator<NpgsqlParameter> { }

    public class PostgreSqlWorker : ASqlWorker<ParametersConstructorForPostgreSql>
    {
        private string _connectionStr;

        public PostgreSqlWorker(string ConnectionString)
            : base() { _connectionStr = ConnectionString; }

        private NpgsqlConnection _conn;

        protected override IDbConnection Connection
        {
            get
            {
                if (_conn == null) _conn = new NpgsqlConnection(_connectionStr);
                return _conn;
            }
        }
    }
}
