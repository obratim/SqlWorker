using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Common;

namespace SqlWorker
{
    public abstract class AbstractDbParameterConstructors
    {
        public abstract DbParameter Create(String name, Object value, System.Data.DbType? type = null, System.Data.ParameterDirection? direction = null);
    }
}
