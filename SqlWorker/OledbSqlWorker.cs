using System;
using System.Collections.Generic;
using System.Data.OleDb;

using System.Linq;

namespace SqlWorker
{
    /// <summary>
    /// Generator of OleDbParameter objects
    /// </summary>
    public class ParametersConstructorForOledb : IDbParameterCreator
    {
        /// <summary>
        /// Creates an OleDbParameter
        /// </summary>
        /// <param name="name">Parameter name</param>
        /// <param name="value">Parameter value</param>
        /// <param name="type">Parameter DBType, optional</param>
        /// <param name="direction">Parameter direction (Input / Output / InputOutput / ReturnValue), optional</param>
        /// <returns>OleDbParameter instance</returns>
        public System.Data.IDataParameter Create(string name, object value, System.Data.DbType? type = null, System.Data.ParameterDirection? direction = null)
        {
            var result = new OleDbParameter(name, value);
            result.DbType = type ?? result.DbType;
            result.Direction = direction ?? result.Direction;
            return result;
        }
    }

    /// <summary>
    /// Adapter for OLE DB
    /// </summary>
    public class OledbSqlWorker : ASqlWorker<ParametersConstructorForOledb>
    {
        private readonly OleDbConnection _conn;

        /// <summary>
        /// Constructor from connection string
        /// </summary>
        /// <param name="connectionString">The connection string</param>
        /// <param name="reconnectPause">Period for 'ReOpenConnection' method</param>
        public OledbSqlWorker(string connectionString, TimeSpan? reconnectPause = null)
            : base(reconnectPause)
        {
            _conn = new OleDbConnection(connectionString);
        }

        /// <summary>
        /// Database connection
        /// </summary>
        protected override System.Data.IDbConnection Conn
        {
            get { return _conn; }
        }
    }
}