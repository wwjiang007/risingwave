#!/bin/bash

set -e

path=$(dirname "$0")
cd "$path"

if [ ! -f "./my_udf.component.wasm" ]; then
    echo "my_udf.component.wasm not found, please run ./build.sh first"
    exit 1
fi

echo "size of wasm: $(stat -f "%z" my_udf.component.wasm) bytes"
encoded=$(base64 --wrap=0 my_udf.component.wasm)
echo "size of encoded wasm: ${#encoded} bytes"
# debug:   23557258
# release: 12457072

psql -d dev -h i-0987cdadc069c0813.ap-southeast-1.compute.internal -p 4566 -U root -c "DROP FUNCTION IF EXISTS wasm_count_char;"
sql="CREATE FUNCTION wasm_count_char (s varchar, c varchar) RETURNS BIGINT LANGUAGE wasm_v1 USING BASE64 '$encoded';"
# CREATE FUNCTION py_count_char (s varchar, c varchar) RETURNS BIGINT LANGUAGE wasm_v1 AS count_char USING LINK 'http://i-0987cdadc069c0813.ap-southeast-1.compute.internal:8815';
# /ebs/risingwave/src/udf/wit_example/create.sh
echo "$sql" > create.sql
psql -d dev -h i-0987cdadc069c0813.ap-southeast-1.compute.internal -p 4566 -U root -f ./create.sql

# test
psql -d dev -h i-0987cdadc069c0813.ap-southeast-1.compute.internal -p 4566 -U root -c "SELECT wasm_count_char('aabca', 'a');"
psql -d dev -h i-0987cdadc069c0813.ap-southeast-1.compute.internal -p 4566 -U root -c "SELECT wasm_count_char('aabca', 'b');"
psql -d dev -h i-0987cdadc069c0813.ap-southeast-1.compute.internal -p 4566 -U root -c "SELECT wasm_count_char('aabca', 'd');"
