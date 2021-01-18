using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlWorker
{
	public class ParametersConstructorForMySql : ADbParameterCreator<MySql.Data.MySqlClient.MySqlParameter>
    {
		/// <summary>
		/// Set parameter size (for types with variable size)
		/// </summary>
		/// <param name="parameter">The parameter</param>
		/// <param name="size">Parameter size</param>
        protected override void SetSize(MySql.Data.MySqlClient.MySqlParameter parameter, int size)
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

        private MySql.Data.MySqlClient.MySqlConnection _conn;

        protected override IDbConnection Connection
        {
            get
            {
                if (_conn == null) _conn = new MySql.Data.MySqlClient.MySqlConnection(_connectionString);
                return _conn;
            }
        }
    }
}
