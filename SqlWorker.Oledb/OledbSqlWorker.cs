using System;
using System.Collections.Generic;
using System.Data.OleDb;

using System.Linq;

namespace SqlWorker
{
	/// <summary>
	/// Generator of OleDbParameter objects
	/// </summary>
	public class ParametersConstructorForOledb : ADbParameterCreator<OleDbParameter> { }

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