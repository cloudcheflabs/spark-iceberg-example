CREATE TABLE IF NOT EXISTS iceberg.iceberg_db.test_iceberg_orc (
    baseproperties STRUCT<eventtype: string,
                       ts: long,
                       uid: string,
                       version: string>,
    itemid string,
    price long,
    quantity long
)
USING iceberg
PARTITIONED BY (itemid)