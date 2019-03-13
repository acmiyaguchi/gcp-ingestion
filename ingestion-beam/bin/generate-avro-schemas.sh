#!/bin/bash
# Generate a new archive with avro schemas from the mozilla-pipeline-schema repository
set -eou pipefail
set -x

if [ ! -x  "$(command -v jsonschema_transpiler)" ]; then
    echo "jsonschema_transpiler is not installed"
    cargo install --git https://github.com/acmiyaguchi/jsonschema-transpiler.git --branch dev
fi

# find all jsonschemas
tar -xf schemas.tar.gz
schemas=$(find mozilla-pipeline-schemas-gcp-ingestion-tests/schemas -type file -name "*.schema.json")

# assume a new folder basename
outfolder="avro-tests"
mkdir $outfolder

# From a relative path to a jsonschema, transpile an avro schema in a mirrored directory.
# Requires: $outfolder
function generate_avro() {
    orig_path=$1
    # relative path to the root of the archive, using shell parameter expansion to strip the root
    relpath=$(dirname $(echo ${orig_path#*/}))
    avroname="$(basename $orig_path .schema.json).avro.json"
    outpath="$outfolder/$relpath/$avroname"
    
    # create the folder to the new schema
    mkdir -p "$outfolder/$relpath"

    jsonschema_transpiler -f orig_path > $outpath
}

for schema in $schemas 
do
    generate_avro $schema
done
