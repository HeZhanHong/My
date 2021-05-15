CREATE TABLE `ibsdb.t_cdr_k_uli`(
  `c_uli` string, 
  `c_areacode` string, 
  `c_lng` string, 
  `c_lat` string, 
  `c_uli_addr` string)
PARTITIONED BY ( 
  `day` string,
  `hour` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://maxc/apps/hive/warehouse/ibsdb.db/t_cdr_k_uli'
TBLPROPERTIES (
  'transient_lastDdlTime'='1589710065')


CREATE TABLE `ibsdb.quality`(
  `type` string, 
  `spcode` int, 
  `value` bigint)
PARTITIONED BY ( 
  `day` string,
  `hour` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://maxc/apps/hive/warehouse/ibsdb.db/quality'
TBLPROPERTIES (
  'transient_lastDdlTime'='1589718621')


CREATE TABLE `ibsdb.quality_result`(
  `type` string,
  `spcode` int,
  `value` string )
PARTITIONED BY ( 
  `day` string COMMENT ':',
  `hour` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://maxc/apps/hive/warehouse/ibsdb.db/quality_result'
TBLPROPERTIES (
  'transient_lastDdlTime'='1594282641')
