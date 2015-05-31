#!/bin/sh

cat Properties/VersionInfo.base.cs | sed -e "s/\\\$REVNUM\\\$/`hg id -n`/g" -e '/\+/ s/\$DIRTY\$/1/ ; /\+/! s/\$DIRTY\$/0/g ; s/\+//g' -e "s/\\\$BRANCH\\\$/`hg id -b`/g" > Properties/VersionInfo.cs

