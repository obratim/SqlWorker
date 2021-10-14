using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySqlConnector;

namespace SqlWorker
{
    public class ParametersConstructorForMySql : ADbParameterCreator<MySqlParameter>
    {
		/// <summary>
		/// Set parameter size (for types with variable size)
		/// </summary>
		/// <param name="parameter">The parameter</param>
		/// <param name="size">Parameter size</param>
        protected override void SetSize(MySqlParameter parameter, int size)
        {
            parameter.Size = size;
        }
    }

    public class MySqlWorker
#if NETSTANDARD2_1
    : Async.ASqlWorkerAsync<ParametersConstructorForMySql>
#else
    : ASqlWorker<ParametersConstructorForMySql>
#endif
    {
        private string _connectionString;

        public MySqlWorker(string connectionString)
            : base()
        {
            _connectionString = connectionString;
        }

        private MySqlConnection _conn;

        protected override IDbConnection Connection
        {
            get
            {
                if (_conn == null) _conn = new MySqlConnection(_connectionString);
                return _conn;
            }
        }
    }
}
