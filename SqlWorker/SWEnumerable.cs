using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Common;
using System.Data;

namespace SqlWorker
{
    public abstract partial class ASqlWorker<TPC> where TPC : AbstractDbParameterConstructors, new()
    {
        protected class DbIer<T> : DbEnumerator, IEnumerator<T>
        {
            public class CommandReleasedEventArgs : EventArgs { public int cmdhash { get; set; } }
            public event Action<object, CommandReleasedEventArgs> CommandReleased;

            DbCommand cmd; DbDataReader dr; int cmdhash;
            Func<DbDataReader, T> converter;
            Func<DbDataReader, bool> moveNextModifier;
            public DbIer(DbCommand cmd, DbDataReader dr, Func<DbDataReader, T> converter, Func<DbDataReader, bool> moveNextModifier = null)
                : base(dr, true)
            {
                this.cmd = cmd;
                this.dr = dr;
                this.converter = converter;
                this.moveNextModifier = moveNextModifier == null ? (reader => reader.Read()) : moveNextModifier;
                cmdhash = cmd.GetHashCode();
            }

            bool hascurrent = false;
            T current;

            #region Члены IEnumerator<T>

            public new T Current
            {
                get
                {
                    if (!hascurrent)
                    { current = converter(dr); }
                    return current;
                }
            }

            new public bool MoveNext()
            {
                hascurrent = false;
                return moveNextModifier(dr);
            }

            #endregion

            #region Члены IDisposable

            public void Dispose()
            {
                dr.Close();
                dr.Dispose();
                cmd.Dispose();
                if (CommandReleased != null) CommandReleased(this, new CommandReleasedEventArgs() { cmdhash = this.cmdhash });
            }

            #endregion
        }

        protected class DbIe<T> : IEnumerable<T>
        {
            DbIer<T> enumerator;

            public DbIe(ASqlWorker<TPC> this_sw, String Command, Func<DbDataReader, T> todo, DbParametersConstructor vals = null, int? timeout = null, Func<DbDataReader, bool> moveNextModifier = null)
            {
                vals = vals ?? DbParametersConstructor.emptyParams;
                ASqlWorker<TPC>.SqlParameterNullWorkaround(vals);
                DbCommand cmd = this_sw.Conn.CreateCommand();
                if (timeout.HasValue) cmd.CommandTimeout = timeout.Value;
                cmd.CommandText = QueryWithParams(Command, vals);
                cmd.Parameters.AddRange(vals);
                cmd.Transaction = this_sw._transaction;
                if (this_sw.Conn.State != ConnectionState.Open) this_sw.Conn.Open();
                DbDataReader dr = cmd.ExecuteReader();

                enumerator = new DbIer<T>(cmd, dr, todo, moveNextModifier);
                this_sw.cmdHashes.Add(cmd.GetHashCode());

                enumerator.CommandReleased += (sender, e) =>
                {
                    this_sw.cmdHashes.Remove(e.cmdhash);
                    if (this_sw.cmdHashes.Count == 0 && !this_sw.TransactionIsOpened)
                        this_sw.Conn.Close();
                };
            }

            public System.Collections.Generic.IEnumerator<T> GetEnumerator()
            { return enumerator; }

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            { return enumerator; }

        }

    }
}
