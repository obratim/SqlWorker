using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SqlWorker
{
    public abstract class TuplesList<T> : List<T>
    {
    }

    public class ValueNameList : TuplesList<Tuple<String, Object>>
    {
        public void Add(String name, Object value) { this.Add(new Tuple<String, Object>(name, value)); }
    }

    public class ValueNameTypeList : TuplesList<Tuple<String, Object, System.Data.DbType>>
    {
        public void Add(String name, Object value, System.Data.DbType type) { this.Add(new Tuple<string, object, System.Data.DbType>(name, value, type)); }
    }
}
