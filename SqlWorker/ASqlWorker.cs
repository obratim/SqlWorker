using System;
using System.Collections.Generic;
using System.Data;

namespace SqlWorker
{
	/// <summary>
	/// Core class, where main logic is realised. Dispose to close connection
	/// </summary>
	/// <typeparam name="TPC">Realisation of AbstractDbParameterConstructors abstract class required</typeparam>
	public abstract partial class ASqlWorker<TPC>
		: IDisposable
		where TPC : IDbParameterCreator, new()
	{
        /// <summary>
        /// Database connection
        /// </summary>
        protected abstract IDbConnection Connection { get; }

        /// <summary>
        /// Timeout for SqlCommand
        /// </summary>
        public int DefaultExecutionTimeout { get; set; } = 30;

		public bool CloseConnectionOnDispose { get; set; } = true;

        #region Transactions

        /// <summary>
        /// Only single one transaction is supported!
        /// </summary>
        virtual public IDbTransaction TransactionBegin(IsolationLevel specificIsolationLevel = IsolationLevel.ReadCommitted)
        {
            if (Connection.State != ConnectionState.Open) Connection.Open();
            return Connection.BeginTransaction(specificIsolationLevel);
        }

		#endregion Transactions

		/// <summary>
		/// Executes specified query
		/// </summary>
		/// <param name="command">Sql string or stored procedure name</param>
		/// <param name="parameters">Query parameters</param>
		/// <param name="timeout">Timeout in seconds</param>
		/// <param name="commandType">Command type: text / stored procedure / TableDirect</param>
		/// <param name="transaction">If transaction was opened, it must be specified</param>
		/// <returns>Result code of the query</returns>
		virtual public int Exec(
			string command,
			DbParametersConstructor parameters = null,
			int? timeout = null,
			CommandType commandType = CommandType.Text,
			IDbTransaction transaction = null)
		{
			int result;
			parameters = parameters ?? DbParametersConstructor.EmptyParams;
            SqlParameterNullWorkaround(parameters);
            using (var cmd = Connection.CreateCommand())
            {
                cmd.CommandText = command;
                foreach (var c in parameters.Parameters) cmd.Parameters.Add(c);
                cmd.CommandType = commandType;
                cmd.Transaction = transaction;
                cmd.CommandTimeout = timeout ?? DefaultExecutionTimeout;
                if (Connection.State != ConnectionState.Open) Connection.Open();
                result = cmd.ExecuteNonQuery();
            }
            return result;
		}

        /// <summary>
        /// Performs ExecuteReader for specified command, performs specified delegate on result, than disposes datareader and command
        /// </summary>
        /// <typeparam name="T">Result type</typeparam>
        /// <param name="command">Sql string or stored procedure name</param>
        /// <param name="transformFunction">Delegate for operating whith result datareader</param>
        /// <param name="parameters">Query parameters</param>
        /// <param name="timeout">Timeout in seconds</param>
        /// <param name="commandType">Command type: text / stored procedure / TableDirect</param>
        /// <param name="transaction">If transaction was opened, it must be specified</param>
        /// <returns>T-object, result of delegate execution</returns>
        virtual public T ManualProcessing<T>(
            string command,
            Func<IDataReader, T> transformFunction,
            DbParametersConstructor parameters = null,
            int? timeout = null,
            CommandType commandType = CommandType.Text,
			CommandBehavior commandBehavior = CommandBehavior.Default,
            IDbTransaction transaction = null)
        {
            parameters = parameters ?? DbParametersConstructor.EmptyParams;
            SqlParameterNullWorkaround(parameters);
            T result;
            using (var cmd = Connection.CreateCommand())
            {
                cmd.CommandTimeout = timeout ?? DefaultExecutionTimeout;
                cmd.CommandType = commandType;
                cmd.CommandText = command;
                foreach (var c in parameters.Parameters) cmd.Parameters.Add(c);
                cmd.Transaction = transaction;
                if (Connection.State != ConnectionState.Open) Connection.Open();
                using (var dr = cmd.ExecuteReader(commandBehavior))
                {
                    result = transformFunction(dr);
                }
            }
            return result;
        }

        /// <summary>
        /// Return IEnumerable with results
        /// </summary>
        /// <typeparam name="T">Generic resulting type</typeparam>
        /// <param name="command">SQL command; in case of stored procedure this parameter stores only Proc name, commandType must be specified then</param>
        /// <param name="transformFunction">Delegate to recive T from DataReader</param>
        /// <param name="parameters">Values of parameters (if necessary)</param>
        /// <param name="timeout">Timeout</param>
        /// <param name="commandType">Type of batch</param>
        /// <param name="transaction">The transaction, inside of wich the command will be executed</param>
        /// <returns>Consequentially readed data</returns>
        virtual public IEnumerable<T> Query<T>(
            string command,
            Func<IDataReader, T> transformFunction,
            DbParametersConstructor parameters = null,
            int? timeout = null,
            CommandType commandType = CommandType.Text,
            IDbTransaction transaction = null)
        {
            parameters = parameters ?? DbParametersConstructor.EmptyParams;
            SqlParameterNullWorkaround(parameters);
            using (var cmd = Connection.CreateCommand())
            {
                cmd.CommandTimeout = timeout ?? DefaultExecutionTimeout;
                cmd.CommandType = commandType;
                cmd.CommandText = command;
                foreach (var c in parameters.Parameters) cmd.Parameters.Add(c);
                cmd.Transaction = transaction;
                if (this.Connection.State != ConnectionState.Open) Connection.Open();
                using (var dr = cmd.ExecuteReader(CommandBehavior.SingleResult))
                {
                    while (dr.Read())
                    {
                        yield return transformFunction(dr);
                    }
                }
            }
        }
#if NETSTANDARD2_1
		/// <summary>
		/// Return IAsyncEnumerable with results
		/// </summary>
		/// <typeparam name="T">Generic resulting type</typeparam>
		/// <param name="command">SQL command; in case of stored procedure this parameter stores only Proc name, commandType must be specified then</param>
		/// <param name="transformFunction">Delegate to recive T from DataReader</param>
		/// <param name="parameters">Values of parameters (if necessary)</param>
		/// <param name="timeout">Timeout</param>
		/// <param name="commandType">Type of batch</param>
		/// <param name="transaction">The transaction, inside of wich the command will be executed</param>
		/// <returns>Consequentially readed data</returns>
		public abstract IAsyncEnumerable<T> QueryAsync<T>(
			string command,
			Func<IDataReader, T> transformFunction,
			DbParametersConstructor parameters = null,
			int? timeout = null,
			CommandType commandType = CommandType.Text,
			IDbTransaction transaction = null);
#endif

        /// <summary>
        /// Executes query and returns DataTable with results
        /// </summary>
        /// <param name="query">Sql string or stored procedure name</param>
        /// <param name="parameters">Query parameters</param>
        /// <param name="timeout">Timeout in seconds</param>
        /// <param name="commandType">Command type: text / stored procedure / TableDirect</param>
        /// <param name="transaction">If transaction was opened, it must be specified</param>
        /// <returns>The DataTable with results</returns>
        virtual public DataTable GetDataTable(
            string query,
            DbParametersConstructor parameters = null,
            int? timeout = null,
            CommandType commandType = CommandType.Text,
            IDbTransaction transaction = null)
        {
            return ManualProcessing(query, dr =>
            {
                var dt = new DataTable();
                dt.Load(dr);
                return dt;
            },
            parameters, timeout, commandType, CommandBehavior.SingleResult, transaction);
        }

        #region IDisposable members

        /// <summary>
        /// Closes database connection
        /// </summary>
        public virtual void Dispose()
        {
			if (CloseConnectionOnDispose && Connection.State != ConnectionState.Closed && Connection.State != ConnectionState.Broken)
				Connection.Close();
            Connection.Dispose();
        }

        #endregion IDisposable members
	}
}
