# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# The free tier of Confluent Cloud does not alow for programatic
# topic creation
# $ kafka-create-topic topic=bytes

# TODO(def-) Reenable when Confluent Kafka is working with Testdrive again
$ skip-if
SELECT true

$ kafka-ingest format=bytes key-terminator=: key-format=bytes topic=bytes repeat=100
abc:abc

> DROP SOURCE IF EXISTS kafka_bytes CASCADE;
> DROP CONNECTION IF EXISTS kafka_conn;
> DROP SECRET IF EXISTS confluent_username
> DROP SECRET IF EXISTS confluent_password

> CREATE SECRET confluent_username AS '${arg.confluent-api-key}';

> CREATE SECRET confluent_password AS '${arg.confluent-api-secret}';

> CREATE CONNECTION kafka_conn TO KAFKA (
  BROKER '${testdrive.kafka-addr}',
  SASL MECHANISMS = 'PLAIN',
  SASL USERNAME = SECRET confluent_username,
  SASL PASSWORD = SECRET confluent_password
  );

> CREATE SOURCE kafka_bytes
  IN CLUSTER canary_sources
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-bytes-${testdrive.seed}');

> CREATE TABLE kafka_bytes_tbl FROM SOURCE kafka_bytes (REFERENCE "testdrive-bytes-${testdrive.seed}")
  FORMAT BYTES
  ENVELOPE NONE;

> CREATE MATERIALIZED VIEW kafka_bytes_view AS SELECT COUNT(*) AS cnt FROM kafka_bytes_tbl;

> CREATE DEFAULT INDEX ON kafka_bytes_view;

> SELECT cnt > 0 from kafka_bytes_view
true
