using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

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
        protected abstract IDbConnection Conn { get; }

        /// <summary>
        /// Period for 'ReOpenConnection' method
        /// </summary>
        public TimeSpan ReConnectPause { get; set; }

        /// <summary>
        /// Default value if 'ReConnectPause' wasn't specified
        /// </summary>
        protected static readonly TimeSpan DefaultReconnectPause = new TimeSpan(0, 2, 0);

        /// <summary>
        /// DateTime of last disconnect
        /// </summary>
        protected DateTime? LastDisconnect;

        /// <summary>
        /// Timeout for SqlCommand
        /// </summary>
        public int DefaultExecutionTimeout { get; set; } = 30;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="reconnectPause">Period for 'ReOpenConnection' method. If null, default will be setted</param>
        public ASqlWorker(TimeSpan? reconnectPause = null)
        {
            ReConnectPause = reconnectPause ?? DefaultReconnectPause;
            LastDisconnect = DateTime.Now - reconnectPause;
        }

        /// <summary>
        /// Serves to reopen connection after 'ReConnectPause' time after last disconnect
        /// </summary>
        /// <returns>True - connection was opened</returns>
        public virtual bool ReOpenConnection()
        {
            if (Conn.State == ConnectionState.Open) return true;
            if (LastDisconnect == null)
            {
                LastDisconnect = DateTime.Now;
                return false;
            }

            if (DateTime.Now - LastDisconnect < ReConnectPause) return false;

            LastDisconnect = null;
            if (Conn.State != ConnectionState.Closed)
            {
                Conn.Close();
            }
            Conn.Open();

            return Conn.State == ConnectionState.Open;
        }

        #region Transactions

        /// <summary>
        /// Only single one transaction is supported!
        /// </summary>
        virtual public IDbTransaction TransactionBegin(IsolationLevel specificIsolationLevel = IsolationLevel.ReadCommitted)
        {
            if (Conn.State != ConnectionState.Open) Conn.Open();
            return Conn.BeginTransaction(specificIsolationLevel);
        }

        #endregion Transactions

        /// <summary>
        /// Shourtcat for ExecuteNonQuery. Executes specified query
        /// </summary>
        /// <param name="command">Sql string or stored procedure name</param>
        /// <param name="vals">Query parameters</param>
        /// <param name="timeout">Timeout in seconds</param>
        /// <param name="commandType">Command type: text / stored procedure / TableDirect</param>
        /// <param name="transaction">If transaction was opened, it must be specified</param>
        /// <returns>Result code of the query</returns>
        virtual public int Exec(
            string command,
            DbParametersConstructor vals = null,
            int? timeout = null,
            CommandType commandType = CommandType.Text,
            IDbTransaction transaction = null)
        {
            int result;
            vals = vals ?? DbParametersConstructor.EmptyParams;
            SqlParameterNullWorkaround(vals);
            using (var cmd = Conn.CreateCommand())
            {
                cmd.CommandText = command;
                foreach (var c in vals.Parameters) cmd.Parameters.Add(c);
                cmd.CommandType = commandType;
                cmd.Transaction = transaction;
                cmd.CommandTimeout = timeout ?? DefaultExecutionTimeout;
                if (Conn.State != ConnectionState.Open) Conn.Open();
                result = cmd.ExecuteNonQuery();
            }
            return result;
        }

        /// <summary>
        /// Executes specified query
        /// </summary>
        /// <param name="command">Sql string or stored procedure name</param>
        /// <param name="vals">Query parameters</param>
        /// <param name="timeout">Timeout in seconds</param>
        /// <param name="commandType">Command type: text / stored procedure / TableDirect</param>
        /// <param name="transaction">If transaction was opened, it must be specified</param>
        /// <returns>Result code of the query</returns>
        [Obsolete]
        virtual public int ExecuteNonQuery(
            string command,
            DbParametersConstructor vals = null,
            int? timeout = null,
            CommandType commandType = CommandType.Text,
            IDbTransaction transaction = null)
        {
            return Exec(command, vals, timeout, commandType, transaction);
        }


        /// <summary>
        /// Inserts values into table. Warning: query is partly prepared by string concatenation, so don't use it with non-constant tableName parameter
        /// </summary>
        /// <param name="tableName">Target table</param>
        /// <param name="vals">Values to insert</param>
        /// <param name="returnIdentity">If true, returns result of `SCOPE_IDENTITY()`, if false, returns number of rows processed</param>
        /// <param name="timeout">Timeout in seconds</param>
        /// <param name="transaction">If transaction was opened, it must be specified</param>
        /// <returns>Returns count of inserted rows if 'returnIdentity'=false or id of last inserted row (result of SCOPE_IDENTITY build-in function) if true</returns>
        virtual public int InsertValues(
            string tableName,
            DbParametersConstructor vals,
            bool returnIdentity = false,
            int? timeout = null,
            IDbTransaction transaction = null)
        {
            SqlParameterNullWorkaround(vals);

            string q = "INSERT INTO " + tableName + " (" + vals[0].ParameterName;

            for (int i = 1; i < vals.Count(); ++i)
                q += ", " + vals[i].ParameterName;

            q += ") VALUES (@" + vals[0].ParameterName;

            for (int i = 1; i < vals.Count(); ++i)
                q += ", @" + vals[i].ParameterName;

            q += ");";

            return !returnIdentity ?
                Exec(q, vals, timeout, transaction: transaction) :
                decimal.ToInt32(ManualProcessing(
                q + " ; select SCOPE_IDENTITY()",
                r => { r.Read(); return r.GetDecimal(0); },
                vals, timeout, transaction: transaction));
        }

        /// <summary>
        /// Updates values
        /// </summary>
        /// <param name="tableName">Target table</param>
        /// <param name="values">Values to update</param>
        /// <param name="condition">Values, that specifies what rows to update</param>
        /// <param name="timeout">Timeout in seconds</param>
        /// <param name="transaction">If transaction was opened, it must be specified</param>
        /// <returns>Count of updated values</returns>
        virtual public int UpdateValues(
            string tableName,
            DbParametersConstructor values,
            DbParametersConstructor condition = null,
            int? timeout = null,
            IDbTransaction transaction = null)
        {
            SqlParameterNullWorkaround(values);
            condition = condition ?? DbParametersConstructor.EmptyParams;

            string q = "UPDATE " + tableName + " SET " + values[0].ParameterName + " = @" + values[0].ParameterName;

            for (int i = 1; i < values.Count(); ++i)
                q += ", " + values[i].ParameterName + " = @" + values[i].ParameterName;

            if (condition.Count() > 0)
                q += " WHERE " + condition[0].ParameterName + " = @" + condition[0].ParameterName;

            for (int i = 1; i < condition.Count(); ++i)
                q += " AND " + condition[i].ParameterName + " = @" + condition[i].ParameterName;

            List<IDataParameter> param = new List<IDataParameter>(values.Parameters);
            param.AddRange(condition.Parameters);
            return Exec(q, param.ToArray(), timeout, transaction: transaction);
        }

        /// <summary>
        /// Updates values. Warning: query is partly prepared by string concatenation, so don't use it with non-constant tableName parameter
        /// </summary>
        /// <param name="tableName">Target table</param>
        /// <param name="values">Values to update</param>
        /// <param name="condition">string sql condition that will be placed into `where {}` clause</param>
        /// <param name="timeout">Timeout in seconds</param>
        /// <param name="transaction">If transaction was opened, it must be specified</param>
        /// <returns>Count of updated values</returns>
        virtual public int UpdateValues(
            string tableName,
            DbParametersConstructor values,
            string condition,
            int? timeout = null,
            IDbTransaction transaction = null)
        {
            SqlParameterNullWorkaround(values);

            string q = "UPDATE " + tableName + " SET " + values[0].ParameterName + " = @" + values[0].ParameterName;

            for (int i = 1; i < values.Count(); ++i)
                q += ", " + values[i].ParameterName + " = @" + values[i].ParameterName;

            if (!string.IsNullOrWhiteSpace(condition))
                q += " WHERE " + condition;

            return Exec(q, values, timeout, transaction: transaction);
        }

        /// <summary>
        /// Performs ExecuteReader for specified command, performs specified delegate on result, than disposes datareader and command
        /// </summary>
        /// <typeparam name="T">Result type</typeparam>
        /// <param name="command">Sql string or stored procedure name</param>
        /// <param name="jobToDo">Delegate for operating whith result datareader</param>
        /// <param name="vals">Query parameters</param>
        /// <param name="timeout">Timeout in seconds</param>
        /// <param name="commandType">Command type: text / stored procedure / TableDirect</param>
        /// <param name="transaction">If transaction was opened, it must be specified</param>
        /// <returns>T-object, result of delegate execution</returns>
        virtual public T ManualProcessing<T>(
            string command,
            Func<IDataReader, T> jobToDo,
            DbParametersConstructor vals = null,
            int? timeout = null,
            CommandType commandType = CommandType.Text,
            IDbTransaction transaction = null)
        {
            vals = vals ?? DbParametersConstructor.EmptyParams;
            SqlParameterNullWorkaround(vals);
            T result;
            using (var cmd = Conn.CreateCommand())
            {
                cmd.CommandTimeout = timeout ?? DefaultExecutionTimeout;
                cmd.CommandType = commandType;
                cmd.CommandText = command;
                foreach (var c in vals.Parameters) cmd.Parameters.Add(c);
                cmd.Transaction = transaction;
                if (Conn.State != ConnectionState.Open) Conn.Open();
                using (var dr = cmd.ExecuteReader())
                {
                    result = jobToDo(dr);
                }
            }
            return result;
        }

        /// <summary>
        /// Return IEnumerable with results
        /// </summary>
        /// <typeparam name="T">Generic resulting type</typeparam>
        /// <param name="command">SQL command; in case of stored procedure this parameter stores only Proc name, commandType must be specified then</param>
        /// <param name="jobToDo">Delegate to recive T from DataReader</param>
        /// <param name="vals">Values of parameters (if necessary)</param>
        /// <param name="timeout">Timeout</param>
        /// <param name="commandType">Type of batch</param>
        /// <param name="transaction">The transaction, inside of wich the command will be executed</param>
        /// <returns>Consequentially readed data</returns>
        virtual public IEnumerable<T> Query<T>(
            string command,
            Func<IDataReader, T> jobToDo,
            DbParametersConstructor vals = null,
            int? timeout = null,
            CommandType commandType = CommandType.Text,
            IDbTransaction transaction = null)
        {
            vals = vals ?? DbParametersConstructor.EmptyParams;
            SqlParameterNullWorkaround(vals);
            using (var cmd = Conn.CreateCommand())
            {
                cmd.CommandTimeout = timeout ?? DefaultExecutionTimeout;
                cmd.CommandType = commandType;
                cmd.CommandText = command;
                foreach (var c in vals.Parameters) cmd.Parameters.Add(c);
                cmd.Transaction = transaction;
                if (this.Conn.State != ConnectionState.Open) Conn.Open();
                using (var dr = cmd.ExecuteReader())
                {
                    while (dr.Read())
                    {
                        yield return jobToDo(dr);
                    }
                }
            }
        }

        /// <summary>
        /// Return IEnumerable with results
        /// </summary>
        /// <typeparam name="T">Generic resulting type</typeparam>
        /// <param name="command">SQL command; in case of stored procedure this parameter stores only Proc name, commandType must be specified then</param>
        /// <param name="jobToDo">Delegate to recive T from DataReader</param>
        /// <param name="vals">Values of parameters (if necessary)</param>
        /// <param name="timeout">Timeout</param>
        /// <param name="commandType">Type of batch</param>
        /// <param name="transaction">The transaction, inside of wich the command will be executed</param>
        /// <returns>Consequentially readed data</returns>
        [Obsolete]
        virtual public IEnumerable<T> Select<T>(
            string command,
            Func<IDataReader, T> jobToDo,
            DbParametersConstructor vals = null,
            int? timeout = null,
            CommandType commandType = CommandType.Text,
            IDbTransaction transaction = null)
        {
            return Query(command, jobToDo, vals, timeout, commandType, transaction);
        }

        /// <summary>
        /// Obtain objects from DataReader using reflection
        /// </summary>
        /// <returns>Sequence of T-objects</returns>
        virtual public IEnumerable<T> SelectWithReflection<T>(
            string command,
            DbParametersConstructor vals = null,
            List<string> exceptions = null,
            int? timeout = null,
            CommandType commandType = CommandType.Text,
            IDbTransaction transaction = null
            ) where T : new()
        {
            if (exceptions != null) return Query(command, dr => DataReaderToObj<T>(dr, exceptions), vals, timeout, commandType, transaction);
            else return Query(command, DataReaderToObj<T>, vals, timeout);
        }

        /// <summary>
        /// Converts DataRow to T with reflection, writing exceptions in list
        /// </summary>
        /// <typeparam name="T">Target type</typeparam>
        /// <param name="dr">Source datareader</param>
        /// <param name="errors">List with errors</param>
        /// <returns>T-object</returns>
        virtual public T DataReaderToObj<T>(IDataReader dr, List<string> errors) where T : new()
        {
            T result = new T();
            foreach (System.ComponentModel.PropertyDescriptor i in System.ComponentModel.TypeDescriptor.GetProperties(result))
            {
                try { if (dr[i.Name] != DBNull.Value) i.SetValue(result, dr[i.Name]); }
                catch (Exception e) { errors.Add(e.ToString()); }
            }

            return result;
        }

        /// <summary>
        /// Converts DataRow to T with reflection, throws!
        /// </summary>
        /// <typeparam name="T">Target type</typeparam>
        /// <param name="dr">Source datareader</param>
        /// <returns>T-object</returns>
        virtual public T DataReaderToObj<T>(IDataReader dr) where T : new()
        {
            T result = new T();
            foreach (System.ComponentModel.PropertyDescriptor i in System.ComponentModel.TypeDescriptor.GetProperties(result))
            {
                if (dr[i.Name] != DBNull.Value) i.SetValue(result, dr[i.Name]);
            }

            return result;
        }

        /// <summary>
        /// Executes query and returns DataTable with results
        /// </summary>
        /// <param name="query">Sql string or stored procedure name</param>
        /// <param name="vals">Query parameters</param>
        /// <param name="timeout">Timeout in seconds</param>
        /// <param name="commandType">Command type: text / stored procedure / TableDirect</param>
        /// <param name="transaction">If transaction was opened, it must be specified</param>
        /// <returns>The DataTable with results</returns>
        virtual public DataTable GetDataTable(
            string query,
            DbParametersConstructor vals = null,
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
            vals, timeout, commandType, transaction);
        }

        #region IDisposable members

        /// <summary>
        /// Closes database connection
        /// </summary>
        public virtual void Dispose()
        {
            if (Conn.State != ConnectionState.Closed) Conn.Close();
            Conn.Dispose();
        }

        #endregion IDisposable members
    }
}