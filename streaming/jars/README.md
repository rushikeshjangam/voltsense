# Flink connector JARs (not committed)

This project pins **apache-flink==1.20.1**. The connector/format JARs must match
that Flink version exactly, or PyFlink throws version-mismatch errors at job
submit time.

Download these two into this directory:

```bash
curl -L -o flink-sql-connector-kafka-3.3.0-1.20.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar

curl -L -o flink-sql-avro-confluent-registry-1.20.1.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/1.20.1/flink-sql-avro-confluent-registry-1.20.1.jar
```
