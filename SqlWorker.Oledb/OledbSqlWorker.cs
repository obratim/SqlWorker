using System;
using System.Collections.Generic;
using System.Data.OleDb;

using System.Linq;

namespace SqlWorker
{
	/// <summary>
	/// Generator of OleDbParameter objects
	/// </summary>
	public class ParametersConstructorForOledb : ADbParameterCreator<OleDbParameter>
    {
		/// <summary>
		/// Set parameter size (for types with variable size)
		/// </summary>
		/// <param name="parameter">The parameter</param>
		/// <param name="size">Parameter size</param>
        protected override void SetSize(OleDbParameter parameter, int size)
        {
            parameter.Size = size;
        }
    }

    /// <summary>
    /// Adapter for OLE DB
    /// </summary>
    public class OledbSqlWorker
#if NETSTANDARD2_1
    : Async.ASqlWorkerAsync<ParametersConstructorForOledb>
#else
    : ASqlWorker<ParametersConstructorForOledb>
#endif
    {
        private readonly OleDbConnection _conn;

        /// <summary>
        /// Constructor from connection string
        /// </summary>
        /// <param name="connectionString">The connection string</param>
        public OledbSqlWorker(string connectionString)
            : base()
        {
            _conn = new OleDbConnection(connectionString);
        }

        /// <summary>
        /// Database connection
        /// </summary>
        protected override System.Data.IDbConnection Connection
        {
            get { return _conn; }
        }
    }
}