#! /bin/bash
export SW_DESCRIPTION='Minimalistic API allows developer to most easily perform operations with database. By default, library automatically manages DbConnection, DbTransaction, DbCommand and DbDataReader objects. Developer only writes queries.'
export SW_VERSION="3.1.$(./get-version.sh)"
export SW_NOTES="$(hg parent --template '{desc}')"
export SW_COPYRIGHT="Copyright $(date +%Y)"
export SW_AUTHOR="Viktor A. Koryagin"
export SW_TAGS="sql"

if [ -n "$1" ]; then
    export SW_NOTES=$1
fi

dotnet pack \
    -p:PackageVersion="\"$SW_VERSION\"" \
    -p:PackageReleaseNotes="\"$SW_NOTES\"" \
    -p:Copyright="\"$SW_COPYRIGHT\"" \
    -p:Authors="\"$SW_AUTHOR\"" \
    -p:Description="\"$SW_DESCRIPTION\"" \
    -p:PackageTags="\"$SW_TAGS\"" \
    -o publish/nuget \
    --include-symbols \
    -p:SymbolPackageFormat=snupkg \
    -c Release \
    -nologo
