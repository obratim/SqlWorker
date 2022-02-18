#! /bin/bash

# usage: ./pack "comment"

export SW_DESCRIPTION='Minimalistic API allows developer to most easily perform operations with database. By default, library automatically manages DbConnection, DbTransaction, DbCommand and DbDataReader objects. Developer only writes queries.'
export SW_VERSION="3.2.$(./get-version.sh)"
export SW_NOTES="$(hg parent --template '{desc}')"
export SW_COPYRIGHT="Copyright $(date +%Y)"
export SW_AUTHOR="vkoryagin"
export SW_TAGS="sql"
export SW_PROJECT_URL="https://github.com/obratim/SqlWorker"

if [ -n "$1" ]; then
    export SW_NOTES=$1
fi

dotnet pack \
    -p:PackageVersion="\"$SW_VERSION\"" \
    -p:PackageReleaseNotes="\"$SW_NOTES\"" \
    -p:Copyright="\"$SW_COPYRIGHT\"" \
    -p:Authors="\"$SW_AUTHOR\"" \
    -p:Owners="\"$SW_AUTHOR\"" \
    -p:Description="\"$SW_DESCRIPTION\"" \
    -p:PackageLicenseExpression="MIT" \
    -p:RepositoryUrl="\"$SW_PROJECT_URL\"" \
    -o publish/nuget \
    --include-symbols \
    -p:SymbolPackageFormat=snupkg \
    -c Release \
    -nologo
