#!/bin/bash -ex
REPOS="aosc-os-core aosc-os-abbs"
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ABBS_META="$DIR/../abbs-meta/abbsmeta.py"
DATA_DIR="data/"
if [ ! -d "$DATA_DIR" ]; then mkdir -p "$DATA_DIR"; fi
pushd "$DATA_DIR"
if [ ! -f .gitkeep ]; then touch .gitkeep; fi
for repo in $REPOS; do
    if [ ! -d "$repo.git" ]; then
        git clone --mirror "https://github.com/AOSC-Dev/$repo.git"
    else
        pushd "$repo.git"
        git remote update
        popd
    fi
done
python3 "$ABBS_META" -p . -m . -d abbs.db \
    -b stable -B stable \
    -c base -u 'https://github.com/AOSC-Dev/aosc-os-abbs' -P 1 aosc-os-abbs
pushd "$DIR"
popd
python3 "$DIR/dpkgrepo.py" abbs.db
rm -rf cache.new
mkdir -p cache.new
dbs="abbs.db piss.db"
for repo in $REPOS; do dbs+=" $repo-marks.db"; done
pushd cache.new
for db in $dbs; do
    sqlite3 "../$db" ".backup $db"
    stat --printf="%s " "$db" >> dbhashs
    "dbhash" "$db" >> dbhashs
    gzip -9 --rsyncable "$db"
done
popd
mv cache cache.old
mv cache.new cache
rm -rf cache.old
