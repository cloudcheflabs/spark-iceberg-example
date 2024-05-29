# Spark Iceberg Example

This is spark iceberg examples to show how to load external s3 data like parquet to iceberg table with Iceberg REST catalog.


## Prerequisites

You need the following to run examples.

- Java 17
- Maven 3.x


## Load External S3 Data to Iceberg Table

### Install AWS S3 Client

```agsl
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip";
unzip -o awscliv2.zip;
sudo ./aws/install -u;
```

### Add Profile

```agsl
aws configure --profile=chango set default.s3.signature_version s3v4;
aws configure --profile=chango set aws_access_key_id xxx
aws configure --profile=chango set aws_secret_access_key xxx
aws configure --profile=chango set region xxx
```

### Build Spark Uberjar

```agsl
mvn -e -DskipTests=true clean install shade:shade;
```

### Upload Spark Uberjar to S3

```agsl
aws s3 cp \
/home/chango/spark-iceberg-example/target/spark-iceberg-example-1.0.0-SNAPSHOT-spark-job.jar \
s3://chango-bucket/test-spark-jobs/spark-iceberg-example-1.0.0-SNAPSHOT-spark-job.jar \
--profile=chango \
--endpoint=https://s3.xxx.amazonaws.com \
;
```

### Run Spark Job using MERGE INTO on Spark Cluster

This example shows to load parquet files in external s3 to iceberg table using `MERGE INTO`.

> Before running spark job, you need to create iceberg table which must be fully qualified name with the convention of 
> `iceberg`.`<schema>`.`<table>`.

```agsl
export CHANGO_S3_ACCESS_KEY=xxx
export CHANGO_S3_SECRET_KEY=xxx
export CHANGO_S3_REGION=xxx
export CHANGO_S3_ENDPOINT=https://s3.xxx.amazonaws.com
export CHANGO_S3_BUCKET=chango-bucket
export CHANGO_ICEBERG_REST_CATALOG_URL=http://chango-comp-3.chango.private:8008
export CHANGO_ICEBERG_REST_CATALOG_WAREHOUSE=s3a://$CHANGO_S3_BUCKET/warehouse-rest
export CHANGO_ICEBERG_REST_CATALOG_TOKEN=xxx

export EXTERNAL_S3_ACCESS_KEY=xxx
export EXTERNAL_S3_SECRET_KEY=xxx
export EXTERNAL_S3_REGION=xxx
export EXTERNAL_S3_ENDPOINT=https://xxx.compat.objectstorage.$EXTERNAL_S3_REGION.oraclecloud.com
export EXTERNAL_S3_BUCKET=xxx

export SPARK_MASTER=spark://chango-comp-1.chango.private:7777


# run spark job.
spark-submit \
--master ${SPARK_MASTER} \
--deploy-mode client \
--name load-external-s3-to-iceberg-mergeinto \
--class co.cloudcheflabs.example.spark.component.ImportExternalS3ToIcebergMergeInto \
--conf spark.executorEnv.AWS_REGION=$CHANGO_S3_REGION \
--conf spark.executorEnv.AWS_ACCESS_KEY_ID=${CHANGO_S3_ACCESS_KEY} \
--conf spark.executorEnv.AWS_SECRET_ACCESS_KEY=${CHANGO_S3_SECRET_KEY} \
--conf spark.hadoop.fs.s3a.$CHANGO_S3_BUCKET.access.key=${CHANGO_S3_ACCESS_KEY} \
--conf spark.hadoop.fs.s3a.$CHANGO_S3_BUCKET.secret.key=${CHANGO_S3_SECRET_KEY} \
--conf spark.hadoop.fs.s3a.$CHANGO_S3_BUCKET.endpoint=${CHANGO_S3_ENDPOINT} \
--conf spark.hadoop.fs.s3a.$CHANGO_S3_BUCKET.endpoint.region=$CHANGO_S3_REGION \
--conf spark.hadoop.fs.s3a.connection.ssl.enabled=true \
--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
--conf spark.hadoop.fs.s3a.fast.upload=true \
--conf spark.hadoop.fs.s3a.path.style.access=true \
--conf spark.hadoop.fs.s3a.change.detection.mode=warn \
--conf spark.hadoop.fs.s3a.change.detection.version.required=false \
--conf spark.hadoop.fs.s3a.multiobjectdelete.enable=true \
--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.iceberg.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
--conf spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
--conf spark.sql.catalog.iceberg.uri=$CHANGO_ICEBERG_REST_CATALOG_URL \
--conf spark.sql.catalog.iceberg.warehouse=$CHANGO_ICEBERG_REST_CATALOG_WAREHOUSE \
--conf spark.sql.catalog.iceberg.token=$CHANGO_ICEBERG_REST_CATALOG_TOKEN \
--conf spark.sql.catalog.iceberg.s3.endpoint=$CHANGO_S3_ENDPOINT \
--conf spark.sql.catalog.iceberg.s3.path-style-access=true \
--conf spark.sql.defaultCatalog=iceberg \
--conf spark.driver.cores=1 \
--conf spark.driver.memory=1G \
--conf spark.cores.max=8 \
--conf spark.executor.cores=1 \
--conf spark.executor.memory=1G \
--conf spark.executor.instances=2 \
--conf spark.executor.extraJavaOptions=" \
--add-opens java.base/java.util=ALL-UNNAMED \
--add-opens java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens java.base/sun.security.action=ALL-UNNAMED \
--add-opens java.base/java.nio=ALL-UNNAMED \
--add-opens java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens java.base/java.net=ALL-UNNAMED \
--add-opens java.base/sun.util.calendar=ALL-UNNAMED \
" \
--conf spark.driver.extraJavaOptions=" \
--add-opens java.base/java.util=ALL-UNNAMED \
--add-opens java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens java.base/sun.security.action=ALL-UNNAMED \
--add-opens java.base/java.nio=ALL-UNNAMED \
--add-opens java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens java.base/java.net=ALL-UNNAMED \
--add-opens java.base/sun.util.calendar=ALL-UNNAMED \
" \
s3a://$CHANGO_S3_BUCKET/test-spark-jobs/spark-iceberg-example-1.0.0-SNAPSHOT-spark-job.jar \
$CHANGO_S3_BUCKET \
$CHANGO_S3_ACCESS_KEY \
$CHANGO_S3_SECRET_KEY \
$CHANGO_S3_ENDPOINT \
$CHANGO_S3_REGION \
$CHANGO_ICEBERG_REST_CATALOG_URL \
$CHANGO_ICEBERG_REST_CATALOG_WAREHOUSE \
$CHANGO_ICEBERG_REST_CATALOG_TOKEN \
s3Bucket=$EXTERNAL_S3_BUCKET \
s3AccessKey=$EXTERNAL_S3_ACCESS_KEY \
s3SecretKey=$EXTERNAL_S3_SECRET_KEY \
s3Endpoint=$EXTERNAL_S3_ENDPOINT \
s3Region=$EXTERNAL_S3_REGION \
s3Path=/temp-external-mergeinto-parquet-path \
format=parquet \
icebergSchema=iceberg_db \
icebergTable=test_ctas_mergeinto \
idColumns=orderkey,partkey,suppkey \
;
```

* `s3Path`: parquet file path in external s3.
* `icebergSchema`: target schema of `iceberg` catalog.
* `icebergTable`: target iceberg table.
* `idColumns`: ID columns of source parquet schema and target iceberg table. comma separated.


### Run Spark Job for Append Only on Spark Cluster 

This example shows to load parquet files to iceberg table for append only.

> Before running spark job, you need to create iceberg table which must be fully qualified name with the convention of
> `iceberg`.`<schema>`.`<table>`.

```agsl
export CHANGO_S3_ACCESS_KEY=xxx
export CHANGO_S3_SECRET_KEY=xxx
export CHANGO_S3_REGION=xxx
export CHANGO_S3_ENDPOINT=https://s3.xxx.amazonaws.com
export CHANGO_S3_BUCKET=chango-bucket
export CHANGO_ICEBERG_REST_CATALOG_URL=http://chango-comp-3.chango.private:8008
export CHANGO_ICEBERG_REST_CATALOG_WAREHOUSE=s3a://$CHANGO_S3_BUCKET/warehouse-rest
export CHANGO_ICEBERG_REST_CATALOG_TOKEN=xxx

export EXTERNAL_S3_ACCESS_KEY=xxx
export EXTERNAL_S3_SECRET_KEY=xxx
export EXTERNAL_S3_REGION=xxx
export EXTERNAL_S3_ENDPOINT=https://xxx.compat.objectstorage.$EXTERNAL_S3_REGION.oraclecloud.com
export EXTERNAL_S3_BUCKET=xxx

export SPARK_MASTER=spark://chango-comp-1.chango.private:7777


# run spark job.
spark-submit \
--master ${SPARK_MASTER} \
--deploy-mode client \
--name load-external-s3-to-iceberg \
--class co.cloudcheflabs.example.spark.component.ImportExternalS3ToIceberg \
--conf spark.executorEnv.AWS_REGION=$CHANGO_S3_REGION \
--conf spark.executorEnv.AWS_ACCESS_KEY_ID=${CHANGO_S3_ACCESS_KEY} \
--conf spark.executorEnv.AWS_SECRET_ACCESS_KEY=${CHANGO_S3_SECRET_KEY} \
--conf spark.hadoop.fs.s3a.$CHANGO_S3_BUCKET.access.key=${CHANGO_S3_ACCESS_KEY} \
--conf spark.hadoop.fs.s3a.$CHANGO_S3_BUCKET.secret.key=${CHANGO_S3_SECRET_KEY} \
--conf spark.hadoop.fs.s3a.$CHANGO_S3_BUCKET.endpoint=${CHANGO_S3_ENDPOINT} \
--conf spark.hadoop.fs.s3a.$CHANGO_S3_BUCKET.endpoint.region=$CHANGO_S3_REGION \
--conf spark.hadoop.fs.s3a.connection.ssl.enabled=true \
--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
--conf spark.hadoop.fs.s3a.fast.upload=true \
--conf spark.hadoop.fs.s3a.path.style.access=true \
--conf spark.hadoop.fs.s3a.change.detection.mode=warn \
--conf spark.hadoop.fs.s3a.change.detection.version.required=false \
--conf spark.hadoop.fs.s3a.multiobjectdelete.enable=true \
--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
--conf spark.driver.cores=1 \
--conf spark.driver.memory=1G \
--conf spark.cores.max=8 \
--conf spark.executor.cores=1 \
--conf spark.executor.memory=1G \
--conf spark.executor.instances=2 \
--conf spark.executor.extraJavaOptions=" \
--add-opens java.base/java.util=ALL-UNNAMED \
--add-opens java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens java.base/sun.security.action=ALL-UNNAMED \
--add-opens java.base/java.nio=ALL-UNNAMED \
--add-opens java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens java.base/java.net=ALL-UNNAMED \
--add-opens java.base/sun.util.calendar=ALL-UNNAMED \
" \
--conf spark.driver.extraJavaOptions=" \
--add-opens java.base/java.util=ALL-UNNAMED \
--add-opens java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens java.base/sun.security.action=ALL-UNNAMED \
--add-opens java.base/java.nio=ALL-UNNAMED \
--add-opens java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens java.base/java.net=ALL-UNNAMED \
--add-opens java.base/sun.util.calendar=ALL-UNNAMED \
" \
s3a://$CHANGO_S3_BUCKET/test-spark-jobs/spark-iceberg-example-1.0.0-SNAPSHOT-spark-job.jar \
$CHANGO_S3_BUCKET \
$CHANGO_S3_ACCESS_KEY \
$CHANGO_S3_SECRET_KEY \
$CHANGO_S3_ENDPOINT \
$CHANGO_S3_REGION \
$CHANGO_ICEBERG_REST_CATALOG_URL \
$CHANGO_ICEBERG_REST_CATALOG_WAREHOUSE \
$CHANGO_ICEBERG_REST_CATALOG_TOKEN \
s3Bucket=$EXTERNAL_S3_BUCKET \
s3AccessKey=$EXTERNAL_S3_ACCESS_KEY \
s3SecretKey=$EXTERNAL_S3_SECRET_KEY \
s3Endpoint=$EXTERNAL_S3_ENDPOINT \
s3Region=$EXTERNAL_S3_REGION \
s3Path=/temp-external-parquet-path \
format=parquet \
icebergSchema=iceberg_db \
icebergTable=test_iceberg_parquet_load \
;
```

* `s3Path`: parquet file path in external s3.
* `icebergSchema`: target schema of `iceberg` catalog.
* `icebergTable`: target iceberg table.

