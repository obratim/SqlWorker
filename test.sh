#! /bin/bash
echo '{
    "connectionStringMaster": "Server=localhost;Database=master;User ID=sa;Password=12345678Bb.;Integrated Security=false",
    "connectionString": "Server=localhost;Database=sqlworker_test;User ID=sa;Password=12345678Bb.;Integrated Security=false",
    "recreateDb": false
}' > Tests/bin/Debug/netcoreapp3.1/config.json && dotnet build Tests && dotnet vstest Tests/bin/Debug/netcoreapp3.1/Tests.dll
