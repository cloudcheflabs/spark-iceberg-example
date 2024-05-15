package co.cloudcheflabs.example.spark.component;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class ImportExportHelper {

    public static final String FORMAT_CSV = "csv";

    public static final String FORMAT_JSON = "json";
    public static final String FORMAT_PARQUET = "parquet";
    public static final String FORMAT_ORC = "orc";
    public static enum ExternalS3ParameterMapKeys {

        // external s3.
        S3_BUCKET("s3Bucket"),
        S3_ACCESS_KEY("s3AccessKey"),
        S3_SECRET_KEY("s3SecretKey"),
        S3_ENDPOINT("s3Endpoint"),
        S3_REGION("s3Region"),
        S3_PATH("s3Path"),


        // file format type.
        FORMAT("format"),

        // chango iceberg schema and table.
        ICEBERG_SCHEMA("icebergSchema"),
        ICEBERG_TABLE("icebergTable");

        private String key;

        private ExternalS3ParameterMapKeys(String key) {
            this.key = key;
        }

        public String getKey() {
            return this.key;
        }
    }

    public static enum ChangoS3ParameterMapKeys {

        // chango s3.
        S3_PATH("s3Path"),


        // file format type.
        FORMAT("format"),

        // chango iceberg schema and table.
        ICEBERG_SCHEMA("icebergSchema"),
        ICEBERG_TABLE("icebergTable");

        private String key;

        private ChangoS3ParameterMapKeys(String key) {
            this.key = key;
        }

        public String getKey() {
            return this.key;
        }
    }

    public static SparkSession createSparkSessionForExternalS3(
            String s3AccessKey,
            String s3SecretKey,
            String s3Endpoint,
            String s3Region,
            String s3Bucket,
            boolean isLocal
    ) {
        SparkConf sparkConf = new SparkConf().setAppName("Run Spark using External S3");
        if(isLocal) {
            sparkConf.setMaster("local[2]");
        }
        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();

        Configuration hadoopConfiguration = spark.sparkContext().hadoopConfiguration();
        hadoopConfiguration.set("fs.s3a.bucket." + s3Bucket + ".endpoint", s3Endpoint);
        if(s3Region != null) {
            hadoopConfiguration.set("fs.s3a.bucket." + s3Bucket + ".endpoint.region", s3Region);
        }
        hadoopConfiguration.set("fs.s3a.bucket." + s3Bucket + ".access.key", s3AccessKey);
        hadoopConfiguration.set("fs.s3a.bucket." + s3Bucket + ".secret.key", s3SecretKey);
        hadoopConfiguration.set("fs.s3a.path.style.access", "true");
        hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");

        return spark;
    }


    public static SparkSession createSparkSessionForIcebergRESTCatalog(
            String s3AccessKey,
            String s3SecretKey,
            String s3Endpoint,
            String s3Region,
            String bucket,
            String restUrl,
            String restWarehouse,
            String restToken,
            boolean isLocal
    ) {
        SparkConf sparkConf = new SparkConf().setAppName("Run Spark using Iceberg REST Catalog");
        if(isLocal) {
            sparkConf.setMaster("local[2]");
        }


        // set aws system properties.
        System.setProperty("aws.region", (s3Region != null) ? s3Region : "us-east-1");
        System.setProperty("aws.accessKeyId", s3AccessKey);
        System.setProperty("aws.secretAccessKey", s3SecretKey);

        // iceberg rest catalog.
        sparkConf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
        sparkConf.set("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog");
        sparkConf.set("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.rest.RESTCatalog");
        sparkConf.set("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        sparkConf.set("spark.sql.catalog.iceberg.uri", restUrl);
        sparkConf.set("spark.sql.catalog.iceberg.warehouse", restWarehouse);
        sparkConf.set("spark.sql.catalog.iceberg.token", restToken);
        sparkConf.set("spark.sql.catalog.iceberg.s3.endpoint", s3Endpoint);
        sparkConf.set("spark.sql.catalog.iceberg.s3.path-style-access", "true");
        sparkConf.set("spark.sql.defaultCatalog", "iceberg");

        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();

        Configuration hadoopConfiguration = spark.sparkContext().hadoopConfiguration();
        hadoopConfiguration.set("fs.s3a.bucket." + bucket + ".endpoint", s3Endpoint);
        if(s3Region != null) {
            hadoopConfiguration.set("fs.s3a.bucket." + bucket + ".endpoint.region", s3Region);
        }
        hadoopConfiguration.set("fs.s3a.bucket." + bucket + ".access.key", s3AccessKey);
        hadoopConfiguration.set("fs.s3a.bucket." + bucket + ".secret.key", s3SecretKey);
        hadoopConfiguration.set("fs.s3a.path.style.access", "true");
        hadoopConfiguration.set("fs.s3a.change.detection.mode", "warn");
        hadoopConfiguration.set("fs.s3a.change.detection.version.required", "false");
        hadoopConfiguration.set("fs.s3a.multiobjectdelete.enable", "true");
        hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");

        return spark;
    }
}
