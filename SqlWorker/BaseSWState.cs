using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Common;

namespace SqlWorker
{
    public abstract class AbstractDbParameterConstructors
    {
        public abstract DbParameter By2(String name, Object value);
        public abstract DbParameter By3(String name, Object value, System.Data.DbType type);
    }
}
