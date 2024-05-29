package co.cloudcheflabs.example.spark.component;

import co.cloudcheflabs.example.spark.util.FileUtils;
import co.cloudcheflabs.example.spark.util.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExportIcebergToExternalS3MergeIntoTestRunner {

    @Test
    public void fromIcebergToExternalS3() throws Exception {

        // external s3 properties.
        String externalS3AccessKey = System.getProperty("externalS3AccessKey");
        String externalS3SecretKey = System.getProperty("externalS3SecretKey");
        String externalS3Endpoint = System.getProperty("externalS3Endpoint");
        String externalS3Region = System.getProperty("externalS3Region");
        String externalS3Bucket = System.getProperty("externalS3Bucket");
        String externalS3Path = System.getProperty("externalS3Path");

        // chango s3 properties.
        String s3AccessKey = System.getProperty("s3AccessKey");
        String s3SecretKey = System.getProperty("s3SecretKey");
        String s3Endpoint = System.getProperty("s3Endpoint");
        String s3Region = System.getProperty("s3Region");
        String s3Bucket = System.getProperty("s3Bucket");

        // iceberg rest catalog.
        String restUrl = System.getProperty("restUrl");
        String restWarehouse = System.getProperty("restWarehouse");
        String restToken = System.getProperty("restToken");

        // data format.
        String format = System.getProperty("format");

        String schema = System.getProperty("schema");
        String table = System.getProperty("table");


        // parameter map for external s3 properties.
        Map<String, String> paramMap = new HashMap<>();
        paramMap.put(ImportExportHelper.ExternalS3ParameterMapKeys.S3_ACCESS_KEY.getKey(), externalS3AccessKey);
        paramMap.put(ImportExportHelper.ExternalS3ParameterMapKeys.S3_SECRET_KEY.getKey(),externalS3SecretKey);
        paramMap.put(ImportExportHelper.ExternalS3ParameterMapKeys.S3_ENDPOINT.getKey(), externalS3Endpoint);
        paramMap.put(ImportExportHelper.ExternalS3ParameterMapKeys.S3_REGION.getKey(), externalS3Region);
        paramMap.put(ImportExportHelper.ExternalS3ParameterMapKeys.S3_BUCKET.getKey(), externalS3Bucket);
        paramMap.put(ImportExportHelper.ExternalS3ParameterMapKeys.S3_PATH.getKey(), externalS3Path);
        paramMap.put(ImportExportHelper.ExternalS3ParameterMapKeys.FORMAT.getKey(), format);
        paramMap.put(ImportExportHelper.ExternalS3ParameterMapKeys.ICEBERG_SCHEMA.getKey(), schema);
        paramMap.put(ImportExportHelper.ExternalS3ParameterMapKeys.ICEBERG_TABLE.getKey(), table);

        // print constructed param map.
        System.out.println("constructed param map: " + paramMap);

        ExportIcebergToExternalS3 exportIcebergToExternalS3 =
                new ExportIcebergToExternalS3();
        exportIcebergToExternalS3.run(
                s3Bucket,
                s3AccessKey,
                s3SecretKey,
                s3Endpoint,
                s3Region,
                restUrl,
                restWarehouse,
                restToken,
                paramMap,
                true
        );

        // spark session for external s3.
        SparkSession spark = ImportExportHelper.createSparkSessionForExternalS3(
                externalS3AccessKey,
                externalS3SecretKey,
                externalS3Endpoint,
                externalS3Region,
                externalS3Bucket,
                true
        );

        String path= "s3a://" + externalS3Bucket + externalS3Path;

        Dataset<Row> externalDf = null;
        if(format.equals(ImportExportHelper.FORMAT_CSV)) {
            externalDf = spark.read().csv(path);
        } else if(format.equals(ImportExportHelper.FORMAT_JSON)) {
            externalDf = spark.read().json(path);
        } else if(format.equals(ImportExportHelper.FORMAT_PARQUET)) {
            externalDf = spark.read().parquet(path);
        } else if(format.equals(ImportExportHelper.FORMAT_ORC)) {
            externalDf = spark.read().orc(path);
        } else {
            throw new RuntimeException("Unsupported file format [" + format + "]!");
        }

        externalDf.show(30);
        System.out.println("row count [" + externalDf.count() + "] for the path [" + path + "]");
    }
}
