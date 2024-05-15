package co.cloudcheflabs.example.spark.component;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Map;

public class ImportExternalS3ToIceberg {

    public ImportExternalS3ToIceberg() {
    }

    public void run(
            String s3Bucket,
            String s3AccessKey,
            String s3SecretKey,
            String s3Endpoint,
            String s3Region,
            String restUrl,
            String restWarehouse,
            String restToken,
            Map<String, String> paramMap,
            boolean isLocal
    ) throws Exception {
        // external s3 properties.
        String externalS3AccessKey = paramMap.get(ImportExportHelper.ExternalS3ParameterMapKeys.S3_ACCESS_KEY.getKey());
        String externalS3SecretKey = paramMap.get(ImportExportHelper.ExternalS3ParameterMapKeys.S3_SECRET_KEY.getKey());
        String externalS3Endpoint = paramMap.get(ImportExportHelper.ExternalS3ParameterMapKeys.S3_ENDPOINT.getKey());
        String externalS3Region = paramMap.get(ImportExportHelper.ExternalS3ParameterMapKeys.S3_REGION.getKey());
        String externalS3Bucket = paramMap.get(ImportExportHelper.ExternalS3ParameterMapKeys.S3_BUCKET.getKey());
        String externalS3Path = paramMap.get(ImportExportHelper.ExternalS3ParameterMapKeys.S3_PATH.getKey());

        // file format.
        String format = paramMap.get(ImportExportHelper.ExternalS3ParameterMapKeys.FORMAT.getKey());

        // chango iceberg schema and table.
        String icebergSchema = paramMap.get(ImportExportHelper.ExternalS3ParameterMapKeys.ICEBERG_SCHEMA.getKey());
        String icebergTable = paramMap.get(ImportExportHelper.ExternalS3ParameterMapKeys.ICEBERG_TABLE.getKey());

        String path = null;
        if(externalS3Path.startsWith("s3a://")) {
            path = externalS3Path;
        } else {
            if(externalS3Path.startsWith("/")) {
                path = "s3a://" + externalS3Bucket + externalS3Path;
            } else {
                path = "s3a://" + externalS3Bucket + "/" + externalS3Path;
            }
        }
        System.out.println("path: " + path);

        // ================ from external s3.

        // spark session for external s3.
        SparkSession spark = ImportExportHelper.createSparkSessionForExternalS3(
                externalS3AccessKey,
                externalS3SecretKey,
                externalS3Endpoint,
                externalS3Region,
                externalS3Bucket,
                isLocal
        );

        // read data from external s3.
        Dataset<Row> readDf = null;
        if(format.equals(ImportExportHelper.FORMAT_JSON)) {
            readDf = spark.read().json(path);
        } else if(format.equals(ImportExportHelper.FORMAT_PARQUET)) {
            readDf = spark.read().parquet(path);
        } else if(format.equals(ImportExportHelper.FORMAT_ORC)) {
            readDf = spark.read().orc(path);
        } else {
            throw new RuntimeException("Unsupported file format [" + format + "]!");
        }

        String globalTempTable = icebergSchema + "_" + icebergTable;

        // create global temp view.
        readDf.createGlobalTempView(globalTempTable);

        // ================ to chango iceberg.

        // spark session for chango iceberg rest catalog.
        SparkSession sparkForIceberg = ImportExportHelper.createSparkSessionForIcebergRESTCatalog(
                s3AccessKey,
                s3SecretKey,
                s3Endpoint,
                s3Region,
                s3Bucket,
                restUrl,
                restWarehouse,
                restToken,
                isLocal
        );

        // create dataframe from global temp view table.
        Dataset<Row> newDf = sparkForIceberg.sql("select * from global_temp." + globalTempTable);

        // chango iceberg table.
        String changoIcebergTable = "iceberg" + "." + icebergSchema + "." + icebergTable;
        System.out.println("changoIcebergTable: " + changoIcebergTable);

        StructType schema = sparkForIceberg.table(changoIcebergTable).schema();

        // apply schema to temp view dataframe.
        Dataset<Row> schemaAppliedDf = sparkForIceberg.createDataFrame(newDf.javaRDD(), schema);

        // append external s3 data to chango iceberg table.
        schemaAppliedDf.writeTo(changoIcebergTable).append();
    }


    public static void main(String[] args) {
        String s3Bucket = args[0];
        String s3AccessKey = args[1];
        String s3SecretKey = args[2];
        String s3Endpoint = args[3];
        String s3Region = args[4];
        String restUrl = args[5];
        String restWarehouse = args[6];
        String restToken = args[7];

        // parameter map for external s3 properties.
        Map<String, String> paramMap = new HashMap<>();

        // construct param map.
        for(String arg : args) {
            for(ImportExportHelper.ExternalS3ParameterMapKeys key : ImportExportHelper.ExternalS3ParameterMapKeys.values()) {
                String paramKey = key.getKey();
                if(arg.startsWith(paramKey)) {
                    int index = arg.indexOf("=");
                    String value = arg.substring(index + 1, arg.length());
                    paramMap.put(paramKey, value);
                    break;
                }
            }
        }

        // print constructed param map.
        System.out.println("constructed param map: " + paramMap);


        ImportExternalS3ToIceberg importExternalS3ToIceberg = new ImportExternalS3ToIceberg();
        try {
            importExternalS3ToIceberg.run(
                    s3Bucket,
                    s3AccessKey,
                    s3SecretKey,
                    s3Endpoint,
                    s3Region,
                    restUrl,
                    restWarehouse,
                    restToken,
                    paramMap,
                    false
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
