package co.cloudcheflabs.example.spark.component;

import co.cloudcheflabs.example.spark.util.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ImportExternalS3ToIcebergMergeIntoTestRunner {

    @Test
    public void fromExternalS3ToIceberg() throws Exception {

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
        String idColumns = System.getProperty("idColumns");


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
        paramMap.put(ImportExportHelper.ExternalS3ParameterMapKeys.ID_COLUMNS.getKey(), idColumns);

        // print constructed param map.
        System.out.println("constructed param map: " + paramMap);

        SparkSession sparkForIceberg = ImportExportHelper.createSparkSessionForIcebergRESTCatalog(
                s3AccessKey,
                s3SecretKey,
                s3Endpoint,
                s3Region,
                s3Bucket,
                restUrl,
                restWarehouse,
                restToken,
                true
        );

        // load external parquet to iceberg table.
        ImportExternalS3ToIcebergMergeInto importExternalS3ToIcebergMergeInto =
                new ImportExternalS3ToIcebergMergeInto();
        importExternalS3ToIcebergMergeInto.run(
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

        // show table rows.
        String tableName = "iceberg" + "." + schema + "." + table;
        Dataset<Row> resultDf = sparkForIceberg.table(tableName);
        resultDf.show(30);

        System.out.printf("table %s row count %d\n", tableName, resultDf.count());
    }
}
