CREATE TABLE IF NOT EXISTS iceberg.iceberg_db.test_iceberg_json_without_struct (
    itemid string,
    price long,
    quantity long
)
USING iceberg
PARTITIONED BY (itemid)