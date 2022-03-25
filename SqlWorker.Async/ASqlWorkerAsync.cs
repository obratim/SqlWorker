using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace SqlWorker.Async
{
    /// <summary>
    /// Advanced SqlWorker with async methods
    /// </summary>
    /// <typeparam name="TPC">Implementation of IDbParameterCreator interface</typeparam>
    public abstract class ASqlWorkerAsync<TPC> : ASqlWorker<TPC>, IAsyncDisposable
        where TPC : IDbParameterCreator, new()
    {
        private const string DbConnectionException = "Async calls not supported in this implementation of SqlWorker";

        /// <summary>
        /// Opens transaction asyncroniously
        /// </summary>
        /// <param name="isolationLevel">Isolation level for transaction</param>
        /// <param name="token">Cancellation token for transaction</param>
        /// <returns>Transaction</returns>
        public async ValueTask<DbTransaction> TransactionBeginAsync(IsolationLevel isolationLevel = IsolationLevel.ReadCommitted, CancellationToken token = default(CancellationToken))
        {
            var conn = Connection as DbConnection;
            if (conn == null)
                throw new NotSupportedException(DbConnectionException);

            if (conn.State != ConnectionState.Open)
                await conn.OpenAsync();

            return await conn.BeginTransactionAsync(isolationLevel, token);
        }

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
        public async IAsyncEnumerable<T> QueryAsync<T>(
            string command,
            Func<IDataReader, T> transformFunction,
            DbParametersConstructor parameters = null,
            int? timeout = null,
            CommandType commandType = CommandType.Text,
            DbTransaction transaction = null)
        {
            var conn = Connection as DbConnection;
            if (conn == null)
                throw new NotSupportedException(DbConnectionException);

            parameters ??= DbParametersConstructor.EmptyParams;
            SqlParameterNullWorkaround(parameters);
            await using var cmd = conn.CreateCommand();
            cmd.CommandTimeout = timeout ?? DefaultExecutionTimeout;
            cmd.CommandType = commandType;
            cmd.CommandText = command;
            cmd.Parameters.AddRange(parameters.Parameters);
            cmd.Transaction = transaction;
            if (conn.State != ConnectionState.Open)
                await conn.OpenAsync();
            await using var dr = await (cmd).ExecuteReaderAsync(CommandBehavior.SingleResult);
            while (await dr.ReadAsync())
            {
                yield return transformFunction(dr);
            }
            cmd.Parameters?.Clear();
        }

        /// <summary>
        /// Executes specified query asyncroniously
        /// </summary>
        /// <param name="command">Sql string or stored procedure name</param>
        /// <param name="parameters">Query parameters</param>
        /// <param name="timeout">Timeout in seconds</param>
        /// <param name="commandType">Command type: text / stored procedure / TableDirect</param>
        /// <param name="transaction">If transaction was opened, it must be specified</param>
        /// <returns>Result code of the query</returns>
        public async Task<int> ExecAsync(
            string command,
            DbParametersConstructor parameters = null,
            int? timeout = null,
            CommandType commandType = CommandType.Text,
            DbTransaction transaction = null)
        {
            var conn = Connection as DbConnection;
            if (conn == null)
                throw new NotSupportedException(DbConnectionException);

            parameters = parameters ?? DbParametersConstructor.EmptyParams;
            SqlParameterNullWorkaround(parameters);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = command;
            cmd.Parameters.AddRange(parameters.Parameters);
            cmd.CommandType = commandType;
            cmd.Transaction = transaction;
            cmd.CommandTimeout = timeout ?? DefaultExecutionTimeout;
            if (conn.State != ConnectionState.Open)
                await conn.OpenAsync();
            var result = await cmd.ExecuteNonQueryAsync();
            cmd.Parameters?.Clear();
            return result;
        }

        /// <summary>
        /// Disposes asyncroniously
        /// </summary>
        /// <returns></returns>
        public async ValueTask DisposeAsync()
        {
            var conn = Connection as DbConnection;
            if (conn == null)
            {
                if (CloseConnectionOnDispose && Connection.State != ConnectionState.Closed && Connection.State != ConnectionState.Broken)
                    Connection.Close();

                Connection.Dispose();
            }

            if (CloseConnectionOnDispose && Connection.State != ConnectionState.Closed && Connection.State != ConnectionState.Broken)
                await conn.CloseAsync();

            await conn.DisposeAsync();
        }
    }
}
