#! /bin/bash
echo '{
    "connectionStringMaster": "Server=localhost;Database=master;User ID=sa;Password=12345678Bb.;Integrated Security=false",
    "connectionString": "Server=localhost;Database=sqlworker_test;User ID=sa;Password=12345678Bb.;Integrated Security=false",
    "connectionStringMasterPostgreSql": "User ID=postgres;Host=localhost;",
    "connectionStringPostgreSql": "User ID=galoise;Password=12345;Host=localhost;Database=numbers;",
    "recreateDb": false
}' > Tests/bin/Debug/netcoreapp5/config.json && dotnet build Tests && dotnet vstest Tests/bin/Debug/netcoreapp5/Tests.dll
