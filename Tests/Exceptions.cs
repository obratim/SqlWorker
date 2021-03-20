using System;

namespace Tests.SqlWorker
{
    public class TestException : Exception
    {
        public TestException(string message = null, Exception innerException = null)
            : base(message, innerException)
        {}
    }
}
