#! /bin/bash
dotnet pack -p:PackageVersion=3.1.$(./get-version.sh) -o publish/nuget --include-symbols -c Release /nologo
