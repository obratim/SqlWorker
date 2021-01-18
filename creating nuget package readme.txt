*** create package: ***

rm -rf publish/nuget/ && rm -rf SqlWorker/obj/ && rm -rf SqlWorker.MsSql/obj/ && ./pack.sh "<comment>"

*** publish: ***

... create key at nuget.org ...

dotnet nuget push SqlWorker.3.1.236.nupkg -s https://api.nuget.org/v3/index.json -k oy2bepjwis4odvrod7va6euqlem7yhtutnuk7nf5qzcpyy
dotnet nuget push SqlWorker.MsSql.3.1.236.nupkg -s https://api.nuget.org/v3/index.json -k oy2bepjwis4odvrod7va6euqlem7yhtutnuk7nf5qzcpyy
