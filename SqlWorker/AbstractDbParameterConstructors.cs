using System;
using System.Collections.Generic;
using System.Data.Common;

using System.Linq;

namespace SqlWorker
{
    public abstract class AbstractDbParameterConstructors
    {
        public abstract DbParameter Create(String name, Object value, System.Data.DbType? type = null, System.Data.ParameterDirection? direction = null);
    }
}