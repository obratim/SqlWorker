using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

using System.Linq;

namespace SqlWorker {

    public abstract partial class ASqlWorker<TPC> where TPC : AbstractDbParameterConstructors, new() {

        protected class DbIer<T> : DbEnumerator, IEnumerator<T> {

            public class CommandReleasedEventArgs : EventArgs { public DbCommand cmd { get; set; } }

            public event Action<object, CommandReleasedEventArgs> CommandReleased;

            private DbCommand cmd; private DbDataReader dr;
            private Func<DbDataReader, T> converter;
            private Func<DbDataReader, bool> moveNextModifier;

            public DbIer(DbCommand cmd, DbDataReader dr, Func<DbDataReader, T> converter, Func<DbDataReader, bool> moveNextModifier = null)
                : base(dr, true) {
                this.cmd = cmd;
                this.dr = dr;
                this.converter = converter;
                this.moveNextModifier = moveNextModifier == null ? (reader => reader.Read()) : moveNextModifier;
            }

            private bool hascurrent = false;
            private T current;

            #region Члены IEnumerator<T>

            public new T Current {
                get {
                    if (!hascurrent) { current = converter(dr); }
                    return current;
                }
            }

            new public bool MoveNext() {
                hascurrent = false;
                return moveNextModifier(dr);
            }

            #endregion Члены IEnumerator<T>

            #region Члены IDisposable

            public void Dispose() {
                dr.Close();
                dr.Dispose();
                cmd.Dispose();
                if (CommandReleased != null) CommandReleased(this, new CommandReleasedEventArgs() { cmd = this.cmd });
                GC.SuppressFinalize(this);
            }

            #endregion Члены IDisposable
        }

        protected class DbIe<T> : IEnumerable<T> {
            private DbIer<T> enumerator;

            public DbIe(ASqlWorker<TPC> this_sw, String Command, Func<DbDataReader, T> todo, DbParametersConstructor vals = null, int? timeout = null, Func<DbDataReader, bool> moveNextModifier = null) {
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
                this_sw.cmds.Add(cmd);

                enumerator.CommandReleased += (sender, e) => {
                    this_sw.cmds.Remove(e.cmd);
                    if (this_sw.cmds.Count == 0 && !this_sw.TransactionIsOpened)
                        this_sw.Conn.Close();
                };
            }

            public System.Collections.Generic.IEnumerator<T> GetEnumerator() {
                return enumerator;
            }

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() {
                return enumerator;
            }
        }
    }
}