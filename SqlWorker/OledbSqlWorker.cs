using System;
using System.Collections.Generic;
using System.Data.OleDb;

using System.Linq;

namespace SqlWorker
{
    public class ParameterConstructor_OLEDB : AbstractDbParameterConstructors
    {
        public override System.Data.Common.DbParameter Create(string name, object value, System.Data.DbType? type = null, System.Data.ParameterDirection? direction = null)
        {
            var result = new OleDbParameter(name, value);
            result.DbType = type ?? result.DbType;
            result.Direction = direction ?? result.Direction;
            return result;
        }
    }

    public class OledbSqlWorker : ASqlWorker<ParameterConstructor_OLEDB>
    {
        private OleDbConnection _conn { get; set; }

        public OledbSqlWorker(String connectionString, TimeSpan? reconnectPause = null)
            : base(reconnectPause)
        {
            _conn = new OleDbConnection(connectionString);
        }

        protected override System.Data.Common.DbConnection Conn
        {
            get { return _conn; }
        }
    }
}