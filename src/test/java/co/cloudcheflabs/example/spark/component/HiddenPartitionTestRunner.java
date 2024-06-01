package co.cloudcheflabs.example.spark.component;

import co.cloudcheflabs.example.spark.util.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class HiddenPartitionTestRunner {

    private static Logger LOG = LoggerFactory.getLogger(HiddenPartitionTestRunner.class);


    @Test
    public void addToHiddenPartitionedTable() throws Exception
    {
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

        String icebergSchema = System.getProperty("schema");
        String icebergTable = System.getProperty("table");

        boolean isLocal = true;

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

        // make json list.
        String json = StringUtils.fileToString("data/hidden-partition.json", true);
        String lines[] = json.split("\\r?\\n");
        List<String> jsonList = new ArrayList<>();
        for(String line : lines) {
            jsonList.add(line);
        }

        String tableName = "iceberg" + "." + icebergSchema + "." + icebergTable;

        // make dataframe from json list with schema.
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkForIceberg.sparkContext());
        StructType schema = sparkForIceberg.table(tableName).schema();
        JavaRDD<String> javaRDD = jsc.parallelize(jsonList);
        Dataset<String> jsonDs = sparkForIceberg.createDataset(javaRDD.rdd(), Encoders.STRING());
        Dataset<Row> df = sparkForIceberg.read().json(jsonDs);

        // convert data type according to schema.
        for(StructField structField : schema.fields()) {
            String field = structField.name();
            DataType dataType = structField.dataType();
            df = df.withColumn(field, df.col(field).cast(dataType));
        }

        df.printSchema();

        Dataset<Row> newDf = sparkForIceberg.createDataFrame(df.javaRDD(), schema);
        newDf.writeTo(tableName).append();
    }
}
