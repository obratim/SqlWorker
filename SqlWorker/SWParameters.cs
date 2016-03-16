using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace SqlWorker {

    public abstract class SWParameters<T> : List<T> {
    }

    public class SWParameters : SWParameters<Tuple<String, Object, DbType?, ParameterDirection?>> {

        public void Add(String name, Object value, DbType? type = null, ParameterDirection? direction = null) {
            this.Add(new Tuple<string, object, DbType?, ParameterDirection?>(name, value, type, direction));
        }
    }
}