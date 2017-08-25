// ***********************************************************************
// Copyright (c) 2015 Charlie Poole
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
// 
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
// ***********************************************************************

using NUnit.Framework;
using NUnitLite;
using SqlWorker;
using System.Linq;

namespace NUnitLite.Tests
{
    public class Program
    {
        private static string _dbName = "db";
        private static string _connectionString = $@"Data Source=(LocalDB)\MSSQLLocalDB;AttachDbFilename=""{System.IO.Path.GetFullPath(_dbName)}.mdf"";Integrated Security=True;Connect Timeout=30";

        /// <summary>
        /// The main program executes the tests. Output may be routed to
        /// various locations, depending on the arguments passed.
        /// </summary>
        /// <remarks>Run with --help for a full list of arguments supported</remarks>
        /// <param name="args"></param>
        public static int Main(string[] args)
        {
            return new AutoRun().Execute(args);
        }

        [Test]
        public static void CanInicialise()
        {
            using (var sw = new MsSqlWorker(_connectionString))
            {
            }
        }

        [Test]
        [OneTimeSetUp]
        public static void CanCreateLocalDb()
        {
            System.IO.File.Delete($"{_dbName}.mdf");
            System.IO.File.Delete($"{_dbName}_log.ldf");
            using (var sw = new MsSqlWorker(@"Data Source=.\sqlexpress;Initial Catalog=tempdb; Integrated Security=true;User Instance=True;"))
            {
                Assert.AreEqual(-1, sw.Exec($"CREATE DATABASE {_dbName} ON PRIMARY (NAME='{_dbName}', FILENAME='{System.IO.Path.GetFullPath(_dbName)}.mdf')"));
                Assert.AreEqual(-1, sw.Exec($"EXEC sp_detach_db '{_dbName}', 'true'"));
            }
        }

        [Test]
        public static void CanQueryConst()
        {
            using (var sw = new MsSqlWorker(_connectionString))
            {
                Assert.AreEqual("hello", sw.Query("select 'hello'", dr => dr[0]).Single());
            }
        }
    }
}