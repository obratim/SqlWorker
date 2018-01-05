1. Rebuild SqlWorker project
2. Edit SqlWorker.nuspec if necessary
3. Open NuGet console
4. > cd .\SqlWorker
5. > nuget pack .\SqlWorker.csproj -Properties Configuration=Release

*** publish: ***

6. nuget setApiKey Your-API-Key
7. nuget push YourPackage.nupkg -Source https://api.nuget.org/v3/index.json
