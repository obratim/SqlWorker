using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Npgsql;
using SqlWorker;

namespace Tests.SqlWorker.Npgsql
{
    [TestClass]
    public class TestNpgsqlWorker
    {
        enum Even { Odd = 1, Even = 2};
        
        [Flags]
        enum Dividers : short {
            Two = 0x1,
            Three = 0x2,
            Five = 0x4,
            Seven = 0x8,
            Eleven = 0x10,
            Thirteen = 0x20,
            Seventeen = 0x40,
            Nineteen = 0x80,
            TwentyThree = 0x1_00,
            TwentyNine = 0x2_00,
            ThirtyOne = 0x4_00,
            ThirtySeven = 0x8_00,
            FortyOne = 0x10_00,
            FortyThree = 0x20_00,
            FortySeven = 0x40_00,
            // FiftyThree = 0x80_00,
            // FiftyNine = 0x1_00_00,
            // SixtyOne = 0x2_00_00,
            // SixtySeven = 0x4_00_00,
            // SeventyOne = 0x8_00_00,
            // SeventyThree = 0x10_00_00,
            // SeventyNine = 0x20_00_00,
        };

        private Even IsEven(int n) => (n % 2) switch { 1 => Even.Odd, _ => Even.Even };

        private Dividers? GetDividers(int n)
        {
            Dividers result = 0;

            if (n % 2 == 0) result |= Dividers.Two;
            if (n % 3 == 0) result |= Dividers.Three;
            if (n % 5 == 0) result |= Dividers.Five;
            if (n % 7 == 0) result |= Dividers.Seven;
            if (n % 11 == 0) result |= Dividers.Eleven;
            if (n % 13 == 0) result |= Dividers.Thirteen;
            if (n % 17 == 0) result |= Dividers.Seventeen;
            if (n % 19 == 0) result |= Dividers.Nineteen;
            if (n % 23 == 0) result |= Dividers.TwentyThree;
            if (n % 29 == 0) result |= Dividers.TwentyNine;
            if (n % 31 == 0) result |= Dividers.ThirtyOne;
            if (n % 37 == 0) result |= Dividers.ThirtySeven;
            if (n % 41 == 0) result |= Dividers.FortyOne;
            if (n % 43 == 0) result |= Dividers.FortyThree;
            if (n % 47 == 0) result |= Dividers.FortySeven;
            // if (n % 53 == 0) result |= Dividers.FiftyThree;
            // if (n % 59 == 0) result |= Dividers.FiftyNine;
            // if (n % 61 == 0) result |= Dividers.SixtyOne;
            // if (n % 67 == 0) result |= Dividers.SixtySeven;
            // if (n % 71 == 0) result |= Dividers.SeventyOne;
            // if (n % 73 == 0) result |= Dividers.SeventyThree;
            // if (n % 79 == 0) result |= Dividers.SeventyNine;

            return result > 0 ? result : null;
        }

        private readonly IConfigurationRoot Config;
        private string ConnectionString => Config["connectionStringPostgreSql"];
        private string ConnectionStringMaster => Config["connectionStringMasterPostgreSql"];

		private static HashSet<int> _primes = new HashSet<int>(Enumerable.Range(1, 1000).Where(i => !Enumerable.Range(2, (int)Math.Sqrt(i)).Any(j => i % j == 0)));

        public TestNpgsqlWorker()
        {
            Config = new ConfigurationBuilder().AddJsonFile("config.json").Build();
        }

        [TestInitialize]
        public void TestConfig()
        {
            bool dbExists;

            Assert.IsNotNull(ConnectionStringMaster);
            Assert.IsNotNull(ConnectionString);
            
            using (var sw = new PostgreSqlWorker(ConnectionStringMaster))
            {
                Assert.AreEqual("hello", sw.Query("select 'hello'", dr => dr[0]).Single());

                dbExists = sw.Query(
                    @"SELECT 1 FROM pg_database WHERE datname = 'numbers';",
                    dr => (int)dr[0])
                    .SingleOrDefault() switch
                    {
                        1 => true,
                        _ => false,
                    };
            }

            if (Config["recreateDb"]?.ToLower() == "true" || !dbExists)
            {
                using (var sw = new PostgreSqlWorker(ConnectionStringMaster))
                {
                    if (dbExists)
                        sw.Exec(@"DROP DATABASE numbers;");
                    sw.Exec(@"CREATE DATABASE numbers WITH OWNER = galoise;");
                }
            
                using (var sw = new PostgreSqlWorker(ConnectionString))
                {
                    Assert.AreEqual("hello", sw.Query("select 'hello'", dr => dr[0]).Single());

                    sw.Exec(@"CREATE TABLE numbers
(
    number integer primary key not null,
    square bigint not null,
    sqrt double precision not null,
    is_prime boolean not null,
    as_text varchar(400),
    half integer null,
    is_even integer null,
    dividers smallint null,
    id UUID null,
    insert_timestamp timestamp null
);");

                    sw.Exec(@"
CREATE PROCEDURE get_prime_number (
	primePosition integer,
    INOUT number integer,
    INOUT square bigint,
    INOUT sqrt double precision,
    INOUT rows int)
LANGUAGE 'plpgsql'
AS $$
BEGIN
    SELECT
        n.number,
        n.square,
        n.sqrt
    FROM numbers AS n
    INTO
        number,
        square,
        sqrt
    WHERE n.is_prime = true
    ORDER BY n.number
    LIMIT 1
    OFFSET primePosition - 1;

    GET DIAGNOSTICS rows = ROW_COUNT;
END
$$;");
                    sw.Exec(@"
CREATE PROCEDURE number_name (
    p_number integer,
    INOUT p_name varchar(100))
LANGUAGE 'plpgsql'
AS $$
BEGIN
    SELECT as_text
    FROM numbers AS n
    INTO p_name
    WHERE n.number = p_number;
END
$$;");

                }
            }
        }

        [TestMethod]
        public void CanExec()
        {
            using var sw = new PostgreSqlWorker(ConnectionString);

            sw.Exec("DELETE FROM numbers");
        }

        [TestMethod]
        public void CanExecWithParametersArray()
        {
            using (var sw = new PostgreSqlWorker(ConnectionString))
            {
                var insertsCount = sw.Exec(
                    command: @"insert into numbers values (@number, @square, @sqrt, @is_prime, @as_text)",
                    parameters: new []
                    {
                        new NpgsqlParameter("number", 1),
                        new NpgsqlParameter("square", 1L),
                        new NpgsqlParameter("sqrt", 1.0),
                        new NpgsqlParameter("is_prime", false),
                        new NpgsqlParameter("as_text", "one"),
                    });
                Assert.AreEqual(1, insertsCount);
                
                var inserted = sw.Query(
                    command: @"select * from numbers where number = 1",
                    transformFunction: dr => (
                        number: (int)dr[0],
                        square: (long)dr[1],
                        sqrt: (double)dr[2],
                        is_prime: (bool)dr[3],
                        as_text: dr.GetNullableString(4)
                    ))
                    .Single();
                Assert.AreEqual((1, 1L, 1.0, false, "one"), inserted);
            }
        }

        [TestMethod]
        public void CanExecWithParametersDictionary()
        {
            using (var sw = new PostgreSqlWorker(ConnectionString))
            {
                var insertsCount = sw.Exec(
                    command: @"insert into numbers values (@number, @square, @sqrt, @is_prime, @as_text)",
                    parameters: new Dictionary<string, object> {
                        { "number", 2 },
                        { "square", 4L },
                        { "sqrt", Math.Sqrt(2) },
                        { "is_prime", true },
                        { "as_text", "two" },
                    });
                Assert.AreEqual(1, insertsCount);
                
                var inserted = sw.Query(
                    command: @"select * from numbers where number = 2",
                    transformFunction: dr => (
                        number: (int)dr[0],
                        square: (long)dr[1],
                        sqrt: (double)dr[2],
                        is_prime: (bool)dr[3],
                        as_text: dr.GetNullableString(4)
                    ))
                    .Single();
                Assert.AreEqual((2, 4L, 1.4142135623730951, true, "two"), inserted);
            }
        }

        [TestMethod]
        public void CanExecWithSwParameters()
        {
            using (var sw = new PostgreSqlWorker(ConnectionString))
            {
                var insertsCount = sw.Exec(
                    command: @"insert into numbers values (@number, @square, @sqrt, @is_prime, @as_text)",
                    parameters: new SwParameters() {
                        { "number", 3 },
                        { "square", 9, System.Data.DbType.Int64 },
                        { "sqrt", Math.Sqrt(3), System.Data.DbType.Double, System.Data.ParameterDirection.Input },
                        { "is_prime", true },
                        { "as_text", "three" },
                    });
                Assert.AreEqual(1, insertsCount);
                
                var inserted = sw.Query(
                    command: @"select * from numbers where number = 3",
                    transformFunction: dr => (
                        number: (int)dr[0],
                        square: (long)dr[1],
                        sqrt: (double)dr[2],
                        is_prime: (bool)dr[3],
                        as_text: dr.GetNullableString(4)
                    ))
                    .Single();
                Assert.AreEqual((3, 9L, 1.7320508075688773, true, "three"), inserted);
            }
        }

        [TestMethod]
        public void CanCommitTransaction()
        {
            using (var sw = new PostgreSqlWorker(ConnectionString))
            using (var tran = sw.TransactionBegin())
            {
                var insertsCount = sw.Exec(
                    command: @"insert into numbers values (@number, @square, @sqrt, @is_prime, @as_text)",
                    parameters: new SwParameters
                    {
                        { "number", 4 },
                        { "square", 16L },
                        { "sqrt", 2.0 },
                        { "is_prime", false },
                        { "as_text", "four" },
                    },
                    transaction: tran);
                Assert.AreEqual(1, insertsCount);
                tran.Commit();
            }
            using(var sw = new PostgreSqlWorker(ConnectionString))
            {
                Assert.AreEqual(
                    expected: (4, 16L, 2.0, false, "four"),
                    actual: sw.Query("select * from numbers where number = 4", dr => ((int)dr[0], (long)dr[1], (double)dr[2], (bool)dr[3], (string)dr[4])).Single());
            }
        }

        [TestMethod]
        public void CanRollBackTransaction()
        {
            using (var sw = new PostgreSqlWorker(ConnectionString))
            using (var tran = sw.TransactionBegin())
            {
                var insertsCount = sw.Exec(
                    command: @"insert into numbers values (@number, @square, @sqrt, @is_prime, @as_text)",
                    parameters: new SwParameters
                    {
                        { "number", 100500 },
                        { "square", -1 },
                        { "sqrt", 0.0 },
                        { "is_prime", false },
                        { "as_text", (string)null },
                    },
                    transaction: tran);
                Assert.AreEqual(1, insertsCount);
                tran.Rollback();
            }
            using (var sw = new PostgreSqlWorker(ConnectionString))
            {
                Assert.AreEqual(
                    expected: 0,
                    actual: sw.Query("select COUNT(*) from numbers where number = 100500", dr => (long)dr[0]).Single()); // in PostgreSQL COUNT(*) returns bigint
            }
        }

        [TestMethod]
        public void TransactionRolledBackIfNotCommitted()
        {
            using (var sw = new PostgreSqlWorker(ConnectionString))
            using (var tran = sw.TransactionBegin())
            {
                var insertsCount = sw.Exec(
                    command: @"insert into numbers values (@number, @square, @sqrt, @is_prime, @as_text)",
                    parameters: new SwParameters
                    {
                        { "number", 100500 },
                        { "square", -1 },
                        { "sqrt", 0.0 },
                        { "is_prime", false },
                        { "as_text", (string)null },
                    },
                    transaction: tran);
                Assert.AreEqual(1, insertsCount);
            }
            using (var sw = new PostgreSqlWorker(ConnectionString))
            {
                Assert.AreEqual(
                    expected: 0,
                    actual: sw.Query("select COUNT(*) from numbers where number = 100500", dr => (long)dr[0]).Single()); // in PostgreSQL COUNT(*) returns bigint
            }
        }

        [TestMethod]
        public void CanBulkInsert()
        {
            using (var sw = new PostgreSqlWorker(ConnectionString))
            {
                var copyInTransaction = true;
                void bulkInsertAndCheck(int start, int length, int chunkSize)
                {
                    using var tran = copyInTransaction ? sw.TransactionBegin() : null;

                    var rangeToInsert = Enumerable
                            .Range(start, length)
                            .Select(i => new {
                                number = i,
                                square = (long)i * i,
                                sqrt = Math.Sqrt(i),
                                is_prime = _primes.Contains(i),
                                as_text = i % 7 == 0 ? (string)null : i.ToString(),
                                half = i % 2 != 0 ? default(int?) : i / 2,
                                is_even = IsEven(i),
                                dividers = GetDividers(i),
                                id = Guid.NewGuid(),
                                insert_timestamp = new DateTime(DateTime.UtcNow.Ticks / 10 * 10), // to workaround DateTimeKind comparision and precision loss
                            })
                            .ToArray();

                    sw.BulkCopy(
                        source: rangeToInsert,
                        targetTableName: "numbers");
                    tran?.Commit();

                    var actual = sw.Query(
                            "select * from numbers where number >= @min_number",
                            dr => new {
                                number = (int)dr[0],
                                square = (long)dr[1],
                                sqrt = (double)dr[2],
                                is_prime = (bool)dr[3],
                                as_text = dr.GetNullableString(4),
                                half = dr.GetNullableInt32(5),
                                is_even = (Even)dr[6],
                                dividers = (Dividers?)dr.GetNullableInt16(7),
                                id = dr.GetGuid(8),
                                insert_timestamp = dr.GetDateTime(9),
                            },
                            parameters: new SwParameters { { "min_number", start } })
                            .ToArray();
                    CollectionAssert.AreEquivalent(
                        expected: rangeToInsert,
                        actual: actual);
                }

                bulkInsertAndCheck(5, 3, 1);
                bulkInsertAndCheck(8, 7, 2);
                bulkInsertAndCheck(15, 10, 3);
                bulkInsertAndCheck(25, 11, 3);
                copyInTransaction = false;
                bulkInsertAndCheck(36, 16, 5);
                bulkInsertAndCheck(52, 18, 5);
                bulkInsertAndCheck(70, 20, 5);
                bulkInsertAndCheck(90, 20, 7);
                copyInTransaction = true;
                bulkInsertAndCheck(110, 0, 11);
                bulkInsertAndCheck(110, 10, 11);
                bulkInsertAndCheck(120, 11, 11);
                copyInTransaction = false;
                bulkInsertAndCheck(131, 20, 13);
            }
        }

        [TestMethod]
        public async System.Threading.Tasks.Task CheckAsyncIEnumerable()
        {
            await using var sw = new PostgreSqlWorker(ConnectionString);

            await using var tran = await sw.TransactionBeginAsync();

            var n = 1;
            await foreach (var x in sw.QueryAsync(
                @"select number, square, sqrt, is_prime from numbers n",
                dr => new {
                    number = (int)dr[0],
                    square = (long)dr[1],
                    sqrt = (double)dr[2],
                    is_prime = (bool)dr[3],
                },
                transaction: tran
            ))
            {
                Assert.AreEqual(x.number, n);
                ++n;
            }
        }

        [TestMethod]
        public async System.Threading.Tasks.Task MultipleEnumeration()
        {
            await using var sw = new PostgreSqlWorker(ConnectionString);
            
            var enumeration = sw.Query(
                @"select number, square, sqrt, is_prime from numbers n where n.number < @maxNumber",
                dr => new {
                    i = (int)dr[0],
                    square = (long)dr[1],
                },
                new SwParameters
                {
                    { "maxNumber", 10 },
                }
            );

            for (var i = 1; i <= 2; ++i)
            {
                foreach (var x in enumeration)
                {
                    Assert.AreEqual(x.square, x.i * x.i);
                }
            }
        }

        [TestMethod]
        public async System.Threading.Tasks.Task MultipleAsyncEnumeration()
        {
            await using var sw = new PostgreSqlWorker(ConnectionString);
            
            var enumeration = sw.QueryAsync(
                @"select number, square, sqrt, is_prime from numbers n where n.number < @maxNumber",
                dr => new {
                    i = (int)dr[0],
                    square = (long)dr[1],
                },
                new SwParameters
                {
                    { "maxNumber", 10 },
                }
            );

            for (var i = 1; i <= 2; ++i)
            {
                await foreach (var x in enumeration)
                {
                    Assert.AreEqual(x.square, x.i * x.i);
                }
            }
        }

        [TestMethod]
        public async Task CheckOutputParameters()
        {
            await using var sw = new PostgreSqlWorker(ConnectionString);

            PostgreSqlWorker.DbParametersConstructor args = new SwParameters
            {
                { "primePosition", 1 },
                { "number", 0, System.Data.DbType.Int32, System.Data.ParameterDirection.InputOutput },
                { "square", 0L, DbType.Int64, ParameterDirection.InputOutput },
                { "sqrt", 0.0, DbType.Double, ParameterDirection.InputOutput },
                { "rows", 0, DbType.Int32, ParameterDirection.InputOutput },
            };

            await sw.ExecAsync("CALL get_prime_number(@primePosition, @number, @square, @sqrt, @rows);", args);
            Assert.AreEqual(2, args[1].Value);
            Assert.AreEqual(1, args[4].Value);

            args[0].Value = 2;
            await sw.ExecAsync("CALL get_prime_number(@primePosition, @number, @square, @sqrt, @rows);", args);
            Assert.AreEqual(3, args["number"].Value);
            Assert.AreEqual(1, args["rows"].Value);
            
            Func<int, int, int, Task> assert = async (position, number, result) =>
            {
                args["primePosition"].Value = position;
                await sw.ExecAsync("CALL get_prime_number(@primePosition, @number, @square, @sqrt, @rows);", args);
                Assert.AreEqual(number, args["number"].Value);
                Assert.AreEqual(result, args["rows"].Value);
            };
            
            await assert(3, 5, 1);
            await assert(4, 7, 1);
            await assert(5, 11, 1);
            await assert(6, 13, 1);
            await assert(7, 17, 1);
            await assert(8, 19, 1);
            await assert(9, 23, 1);
            await assert(10, 29, 1);
            await assert(11, 31, 1);
            
            args[0].Value = 100500;
            await sw.ExecAsync("CALL get_prime_number(@primePosition, @number, @square, @sqrt, @rows);", args);
            Assert.AreEqual((int)args["rows"].Value, 0);
        }

        [TestMethod]
        public async Task SizeForSqlParameter()
        {
            await using var sw = new PostgreSqlWorker(ConnectionString);

            PostgreSqlWorker.DbParametersConstructor args = new SwParameters
            {
                { "number", 1 },
                { "name", default(string), DbType.String, ParameterDirection.InputOutput, 100 },
            };

            Func<int, string, Task> assert = async (number, name) => {
                args[0].Value = number;
                await sw.ExecAsync("CALL number_name(@number, @name);", args);
                Assert.AreEqual(args["name"].Value, name);
            };

            await assert(2, "two");
            await assert(3, "three");
        }

        [TestMethod]
        public void BulkCopyCanBeRolledBack()
        {
            using (var sw = new PostgreSqlWorker(ConnectionString))
            {
                using var tran = sw.TransactionBegin();

                sw.BulkCopy(
                    Enumerable.Range(1, 10)
                        .Select(i => new { number = 100500 + i, square = (long)i * i, sqrt = Math.Sqrt(i), is_prime = _primes.Contains(i), as_text = (string)null }),
                    "numbers");

                tran.Rollback();
            }

            using (var sw = new PostgreSqlWorker(ConnectionString))
            {
                var inserted = sw.Query("SELECT COUNT(1) FROM numbers WHERE number >= 100500", dr => (long)dr[0]).Single();
                Assert.AreEqual(inserted, 0L);
            }
        }

        [TestMethod]
        public void BulkCopyWorksWithArrays()
        {
            Console.WriteLine(ConnectionString);
            using var sw = new PostgreSqlWorker(ConnectionString);
            using var tran = sw.TransactionBegin();
            sw.Exec("create temporary table tmp_table(some_array int[], decimal_array decimal[]) on commit drop", transaction: tran);
            var source = Enumerable.Range(0, 10).Select(x => new
            {
                some_array = Enumerable.Range(0, 10).ToArray(),
                decimal_array = Enumerable.Range(0, 10).Select(x => (decimal) x).ToArray()
            });
            sw.BulkCopy(source, "tmp_table");
            var res = sw.Query("select some_array, decimal_array from tmp_table", 
                    dr => new { some_array = dr.GetArray<int>(0), decimal_array = dr.GetArray<decimal>(1) })
                .ToArray();
            Assert.IsTrue(res.Length == 10);
            tran.Rollback();
        }
        
        [TestMethod]
        public void BulkCopyRollesBackOnException()
        {
            try
            {
                using (var sw = new PostgreSqlWorker(ConnectionString))
                {
                    sw.BulkCopy(
                        Enumerable
                            .Range(1, 10)
                            .Select(i => i switch {
                                6 => throw new TestException(),
                                _ => i,
                            })
                            .Select(i => new { number = 100500 + i, square = (long)i * i, sqrt = Math.Sqrt(i), is_prime = _primes.Contains(i), as_text = (string)null }),
                        "numbers");
                }
            }
            catch (TestException)
            {}

            using (var sw = new PostgreSqlWorker(ConnectionString))
            {
                var inserted = sw.Query("SELECT COUNT(1) FROM numbers WHERE number >= 100500", dr => (long)dr[0]).Single();
                Assert.AreEqual(inserted, 0L);
            }
        }
    }
}
