using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace SqlWorker
{
    public abstract partial class ASqlWorker<TPC> : IDisposable where TPC : AbstractDbParameterConstructors, new()
    {
        protected abstract DbConnection Conn { get; }

        public TimeSpan ReConnectPause { get; set; }
        protected TimeSpan DefaultReconnectPause = new TimeSpan(0, 2, 0);
        protected DateTime? LastDisconnect;

        public int DefaultExecutionTimeout = 30;

        /// <summary>
        /// constructor
        /// </summary>
        /// <param name="reconnectPause">if null, default will be setted</param>
        public ASqlWorker(TimeSpan? reconnectPause = null)
        {
            ReConnectPause = reconnectPause ?? DefaultReconnectPause;
            LastDisconnect = DateTime.Now - reconnectPause;
        }

        /// <summary>
        /// serves to reopen connection after 'ReConnectPause' time after last disconnect
        /// </summary>
        /// <returns>true - connection was opened</returns>
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
        /// <param name="command"></param>
        /// <param name="vals"></param>
        /// <param name="timeout"></param>
        /// <param name="commandType"></param>
        /// <param name="transaction"></param>
        /// <returns></returns>
        virtual public int Exec(String command, DbParametersConstructor vals = null, int? timeout = null, CommandType commandType = CommandType.Text, DbTransaction transaction = null)
        {
            return ExecuteNonQuery(command, vals, timeout, commandType, transaction);
        }

        /// <summary>
        /// Executes specified query
        /// </summary>
        /// <param name="command"></param>
        /// <param name="vals"></param>
        /// <param name="timeout"></param>
        /// <param name="commandType"></param>
        /// <param name="transaction"></param>
        /// <returns></returns>
        virtual public int ExecuteNonQuery(String command, DbParametersConstructor vals = null, int? timeout = null, CommandType commandType = CommandType.Text, DbTransaction transaction = null)
        {
            int result;
            vals = vals ?? DbParametersConstructor.emptyParams;
            SqlParameterNullWorkaround(vals);
            using (DbCommand cmd = Conn.CreateCommand())
            {
                cmd.CommandText = command;
                cmd.Parameters.AddRange(vals);
                cmd.CommandType = commandType;
                cmd.Transaction = transaction;
                cmd.CommandTimeout = timeout ?? DefaultExecutionTimeout;
                if (Conn.State != ConnectionState.Open) Conn.Open();
                result = cmd.ExecuteNonQuery();
            }
            return result;
        }


        /// <summary>
        /// Inserts values into table. Warning: query is partly prepared by string concatenation, so don't use it with non-constant tableName parameter
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="vals"></param>
        /// <param name="returnIdentity"></param>
        /// <param name="timeout"></param>
        /// <param name="transaction"></param>
        /// <returns>Returns count of inserted rows if 'returnIdentity'=false or id of last inserted row (result of SCOPE_IDENTITY build-in function) if true</returns>
        virtual public int InsertValues(String tableName, DbParametersConstructor vals, bool returnIdentity = false, int? timeout = null, DbTransaction transaction = null)
        {
            SqlParameterNullWorkaround(vals);

            String q = "INSERT INTO " + tableName + " (" + vals[0].ParameterName;

            for (int i = 1; i < vals.Count(); ++i)
                q += ", " + vals[i].ParameterName;

            q += ") VALUES (@" + vals[0].ParameterName;

            for (int i = 1; i < vals.Count(); ++i)
                q += ", @" + vals[i].ParameterName;

            q += ");";

            return !returnIdentity ?
                ExecuteNonQuery(q, vals, timeout, transaction: transaction) :
                Decimal.ToInt32(ManualProcessing(
                q + " ; select SCOPE_IDENTITY()",
                r => { r.Read(); return r.GetDecimal(0); },
                vals, timeout, transaction: transaction));
        }

        /// <summary>
        /// Updates values. Warning: query is partly prepared by string concatenation, so don't use it with non-constant tableName parameter
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="values"></param>
        /// <param name="condition"></param>
        /// <param name="timeout"></param>
        /// <param name="transaction"></param>
        /// <returns>Count of updated values</returns>
        virtual public int UpdateValues(String tableName, DbParametersConstructor values, DbParametersConstructor condition = null, int? timeout = null, DbTransaction transaction = null)
        {
            SqlParameterNullWorkaround(values);
            condition = condition ?? DbParametersConstructor.emptyParams;

            String q = "UPDATE " + tableName + " SET " + values[0].ParameterName + " = @" + values[0].ParameterName;

            for (int i = 1; i < values.Count(); ++i)
                q += ", " + values[i].ParameterName + " = @" + values[i].ParameterName;

            if (condition.Count() > 0)
                q += " WHERE " + condition[0].ParameterName + " = @" + condition[0].ParameterName;

            for (int i = 1; i < condition.Count(); ++i)
                q += " AND " + condition[i].ParameterName + " = @" + condition[i].ParameterName;

            List<DbParameter> param = new List<DbParameter>(values.parameters);
            param.AddRange(condition.parameters);
            return ExecuteNonQuery(q, param.ToArray(), timeout, transaction: transaction);
        }

        /// <summary>
        /// Updates values. Warning: query is partly prepared by string concatenation, so don't use it with non-constant tableName parameter
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="vals"></param>
        /// <param name="condition"></param>
        /// <param name="timeout"></param>
        /// <param name="transaction"></param>
        /// <returns>Count of updated values</returns>
        virtual public int UpdateValues(String tableName, DbParametersConstructor vals, String condition, int? timeout = null, DbTransaction transaction = null)
        {
            SqlParameterNullWorkaround(vals);

            String q = "UPDATE " + tableName + " SET " + vals[0].ParameterName + " = @" + vals[0].ParameterName;

            for (int i = 1; i < vals.Count(); ++i)
                q += ", " + vals[i].ParameterName + " = @" + vals[i].ParameterName;

            if (!String.IsNullOrWhiteSpace(condition))
                q += " WHERE " + condition;

            return ExecuteNonQuery(q, vals, timeout, transaction: transaction);
        }

        virtual public T ManualProcessing<T>(String command, Func<DbDataReader, T> todo, DbParametersConstructor vals = null, int? timeout = null, CommandType commandType = CommandType.Text, DbTransaction transaction = null)
        {
            vals = vals ?? DbParametersConstructor.emptyParams;
            SqlParameterNullWorkaround(vals);
            T result;
            using (var cmd = Conn.CreateCommand())
            {
                cmd.CommandTimeout = timeout ?? DefaultExecutionTimeout;
                cmd.CommandType = commandType;
                cmd.CommandText = command;
                cmd.Parameters.AddRange(vals);
                cmd.Transaction = transaction;
                if (Conn.State != ConnectionState.Open) Conn.Open();
                using (var dr = cmd.ExecuteReader())
                {
                    result = todo(dr);
                }
            }
            return result;
        }

        /// <summary>
        /// Return IEnumerable with results
        /// </summary>
        /// <typeparam name="T">Generic resulting type</typeparam>
        /// <param name="command">SQL command; in case of stored procedure this parameter stores only Proc name, commandType must be specified then</param>
        /// <param name="todo">delegate to recive T from DataReader</param>
        /// <param name="vals">values of parameters (if necessary)</param>
        /// <param name="timeout">timeout</param>
        /// <param name="commandType">Type of batch</param>
        /// <param name="transaction">the transaction, inside of wich the command will be executed</param>
        /// <returns>consequentially readed data</returns>
        virtual public IEnumerable<T> Select<T>(String command, Func<DbDataReader, T> todo, DbParametersConstructor vals = null, int? timeout = null, CommandType commandType = CommandType.Text, DbTransaction transaction = null)
        {
            vals = vals ?? DbParametersConstructor.emptyParams;
            SqlParameterNullWorkaround(vals);
            using (var cmd = Conn.CreateCommand())
            {
                cmd.CommandTimeout = timeout ?? DefaultExecutionTimeout;
                cmd.CommandType = commandType;
                cmd.CommandText = command;
                cmd.Parameters.AddRange(vals);
                cmd.Transaction = transaction;
                if (this.Conn.State != ConnectionState.Open) Conn.Open();
                using (var dr = cmd.ExecuteReader())
                {
                    while (dr.Read())
                    {
                        yield return todo(dr);
                    }
                }
            }
        }

        /// <summary>
        /// Obtain objects from DataReader using reflection
        /// </summary>
        /// <returns></returns>
        virtual public IEnumerable<T> SelectWithReflection<T>(
            String command,
            DbParametersConstructor vals = null,
            List<String> exceptions = null,
            int? timeout = null,
            CommandType commandType = CommandType.Text,
            DbTransaction transaction = null
            ) where T : new()
        {
            if (exceptions != null) return Select(command, dr => DataReaderToObj<T>(dr, exceptions), vals, timeout, commandType, transaction);
            else return Select(command, dr => DataReaderToObj<T>(dr), vals, timeout);
        }

        /// <summary>
        /// Converts DataRow to T with reflection, writing exceptions in list
        /// </summary>
        virtual public T DataReaderToObj<T>(DbDataReader dr, List<String> errors) where T : new()
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
        virtual public T DataReaderToObj<T>(DbDataReader dr) where T : new()
        {
            T result = new T();
            foreach (System.ComponentModel.PropertyDescriptor i in System.ComponentModel.TypeDescriptor.GetProperties(result))
            {
                if (dr[i.Name] != DBNull.Value) i.SetValue(result, dr[i.Name]);
            }

            return result;
        }

        virtual public DataTable GetDataTable(String query, DbParametersConstructor vals = null, int? timeout = null, CommandType commandType = CommandType.Text, DbTransaction transaction = null)
        {
            return ManualProcessing(query, (dr) =>
            {
                var dt = new DataTable();
                dt.Load(dr);
                return dt;
            },
            vals, timeout, commandType, transaction);
        }

        #region IDisposable members

        public virtual void Dispose()
        {
            if (Conn.State != ConnectionState.Closed) Conn.Close();
            Conn.Dispose();
        }

        #endregion IDisposable members
    }
}