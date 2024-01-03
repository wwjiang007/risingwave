#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

# prepare environment
export CONNECTOR_LIBS_PATH="./connector-node/libs"

while getopts 'p:' opt; do
    case ${opt} in
        p )
            profile=$OPTARG
            ;;
        \? )
            echo "Invalid Option: -$OPTARG" 1>&2
            exit 1
            ;;
        : )
            echo "Invalid option: $OPTARG requires an argument" 1>&2
            ;;
    esac
done
shift $((OPTIND -1))

download_and_prepare_rw "$profile" source

echo "--- Download connector node package"
buildkite-agent artifact download risingwave-connector.tar.gz ./
mkdir ./connector-node
tar xf ./risingwave-connector.tar.gz -C ./connector-node

echo "--- Prepare data"
cp src/connector/src/test_data/simple-schema.avsc ./avro-simple-schema.avsc
cp src/connector/src/test_data/complex-schema.avsc ./avro-complex-schema.avsc
cp src/connector/src/test_data/complex-schema ./proto-complex-schema
cp src/connector/src/test_data/complex-schema.json ./json-complex-schema


echo "--- e2e, ci-1cn-1fe, mysql & postgres cdc"

# import data to mysql
mysql --host=mysql --port=3306 -u root -p123456 < ./e2e_test/source/cdc/mysql_cdc.sql

# import data to postgres
export PGHOST=db PGPORT=5432 PGUSER=postgres PGPASSWORD=postgres PGDATABASE=cdc_test
createdb
psql < ./e2e_test/source/cdc/postgres_cdc.sql

echo "--- starting risingwave cluster"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
cargo make ci-start ci-1cn-1fe-with-recovery

echo "--- inline cdc test"
export MYSQL_HOST=mysql MYSQL_TCP_PORT=3306 MYSQL_PWD=123456
sqllogictest -p 4566 -d dev './e2e_test/source/cdc_inline/**/*.slt'

echo "--- opendal source test"
sqllogictest -p 4566 -d dev './e2e_test/source/opendal/**/*.slt'

echo "--- mysql & postgres cdc validate test"
sqllogictest -p 4566 -d dev './e2e_test/source/cdc/cdc.validate.mysql.slt'
sqllogictest -p 4566 -d dev './e2e_test/source/cdc/cdc.validate.postgres.slt'

# cdc share stream test cases
export MYSQL_HOST=mysql MYSQL_TCP_PORT=3306 MYSQL_PWD=123456
sqllogictest -p 4566 -d dev './e2e_test/source/cdc/cdc.share_stream.slt'

echo "--- mysql & postgres load and check"
sqllogictest -p 4566 -d dev './e2e_test/source/cdc/cdc.load.slt'
# wait for cdc loading
sleep 10
sqllogictest -p 4566 -d dev './e2e_test/source/cdc/cdc.check.slt'

# kill cluster
cargo make kill
echo "> cluster killed "

echo "--- mysql & postgres recovery check"
# insert into mytest database (cdc.share_stream.slt)
mysql --protocol=tcp -u root mytest -e "INSERT INTO products
       VALUES (default,'RisingWave','Next generation Streaming Database'),
              (default,'Materialize','The Streaming Database You Already Know How to Use');
       UPDATE products SET name = 'RW' WHERE id <= 103;
       INSERT INTO orders VALUES (default, '2022-12-01 15:08:22', 'Sam', 1000.52, 110, false);"


# insert new rows
mysql --host=mysql --port=3306 -u root -p123456 < ./e2e_test/source/cdc/mysql_cdc_insert.sql
echo "> inserted new rows into mysql"

psql < ./e2e_test/source/cdc/postgres_cdc_insert.sql
echo "> inserted new rows into postgres"

# start cluster w/o clean-data
unset RISINGWAVE_CI
export RUST_LOG="events::stream::message::chunk=trace,risingwave_stream=debug,risingwave_batch=info,risingwave_storage=info" \

cargo make dev ci-1cn-1fe-with-recovery
echo "> wait for cluster recovery finish"
sleep 20
echo "> check mviews after cluster recovery"
# check results
sqllogictest -p 4566 -d dev './e2e_test/source/cdc/cdc.check_new_rows.slt'

# drop relations
sqllogictest -p 4566 -d dev './e2e_test/source/cdc/cdc_share_stream_drop.slt'

echo "--- Kill cluster"
cargo make ci-kill

echo "--- e2e, ci-1cn-1fe, protobuf schema registry"
export RISINGWAVE_CI=true
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
cargo make ci-start ci-1cn-1fe
python3 -m pip install requests protobuf confluent-kafka
python3 e2e_test/schema_registry/pb.py "message_queue:29092" "http://message_queue:8081" "sr_pb_test" 20
echo "make sure google/protobuf/source_context.proto is NOT in schema registry"
curl --silent 'http://message_queue:8081/subjects'; echo
# curl --silent --head -X GET 'http://message_queue:8081/subjects/google%2Fprotobuf%2Fsource_context.proto/versions' | grep 404
curl --silent 'http://message_queue:8081/subjects' | grep -v 'google/protobuf/source_context.proto'
sqllogictest -p 4566 -d dev './e2e_test/schema_registry/pb.slt'

echo "--- Kill cluster"
cargo make ci-kill

echo "--- e2e, ci-kafka-plus-pubsub, kafka and pubsub source"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
cargo make ci-start ci-pubsub
./scripts/source/prepare_ci_kafka.sh
cargo run --bin prepare_ci_pubsub
sqllogictest -p 4566 -d dev './e2e_test/source/basic/*.slt'
sqllogictest -p 4566 -d dev './e2e_test/source/basic/old_row_format_syntax/*.slt'
sqllogictest -p 4566 -d dev './e2e_test/source/basic/alter/kafka.slt'

echo "--- e2e, kafka alter source"
chmod +x ./scripts/source/prepare_data_after_alter.sh
./scripts/source/prepare_data_after_alter.sh 2
sqllogictest -p 4566 -d dev './e2e_test/source/basic/alter/kafka_after_new_data.slt'

echo "--- e2e, kafka alter source again"
./scripts/source/prepare_data_after_alter.sh 3
sqllogictest -p 4566 -d dev './e2e_test/source/basic/alter/kafka_after_new_data_2.slt'

echo "--- Run CH-benCHmark"
./risedev slt -p 4566 -d dev './e2e_test/ch_benchmark/batch/ch_benchmark.slt'
./risedev slt -p 4566 -d dev './e2e_test/ch_benchmark/streaming/*.slt'
