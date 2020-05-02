*** create package: ***

rm -rf publish/nuget/ && rm -rf SqlWorker/obj/ && rm -rf SqlWorker.MsSql/obj/ && ./pack.sh

*** publish: ***

6. nuget setApiKey Your-API-Key
7. nuget push YourPackage.nupkg -Source https://api.nuget.org/v3/index.json
