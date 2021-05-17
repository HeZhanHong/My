

#cdr指标统计
presto-cli-hive -e "select 0,type,spcode,value,day from ibsdb.quality_result where day=data_format(current_timestamp - interval '1' day,'yyyyMMdd') and type like 'cdr%' and type != 'cdr_machine' and type != 'cdr设备输出' order by type,spcode" | sed 's/"//g'>/tmp/base_cdr_quality.csv
#qxrz指标统计
presto-cli-hive -e "select 0,type,spcode,value,day from ibsdb.quality_result where day=data_format(current_timestamp - interval '1' day,'yyyyMMdd') and type like 'ybrz%' and type != 'ybrz_machine' and type != 'ybrz设备输出' order by type,spcode" | sed 's/"//g'>/tmp/base_ybrz_quality.csv
#基站指标统计
presto-cli-hive -e "select 0,type,spcode,value,day from ibsdb.quality_result where day=data_format(current_timestamp - interval '1' day,'yyyyMMdd') and type like '%基站%' order by type,spcode" | sed 's/"//g'>/tmp/base_station_quality.csv
#设备指标统计
presto-cli-hive -e "select 0,spcode,1,value,case spcode when 235 then 2 when 301 then 1 when 303 then 1 when 305 then 1 when 307 then 2 when 308 then 3 when 309 then 3 when 311 then 1 when 312 then 3 when 313 then 2 when 314 then 1 when 315 then 3 when 316 then 2 when 321 then 1 when 322 then 3 when 323 then 2 when 1001 then 1 when 1002 then 2 when 1003 then 3 else 4 end ,case spcode when 235 then '恒安嘉新' when 301 then '中新赛克' when 303 then '中新赛克' when 305 then '中新赛克' when 307 then '中新赛克' when 308 then '中新赛克' when 309 then '中新赛克' when 311 then '中新赛克' when 312 then '中新赛克' when 313 then '中新赛克' when 314 then '中新赛克' when 315 then '中新赛克' when 316 then '中新赛克' when 321 then '中新赛克' when 322 then '中新赛克' when 323 then '中新赛克' else '无' end,day from ibsdb.quality_result where day=data_format(current_timestamp - interval '1' day,'yyyyMMdd') and type = 'cdr设备输出' order by type,spcode;select 0,spcode,2,value,case spcode when 235 then 2 when 301 then 1 when 302 then 1 when 303 then 1 when 304 then 1 when 305 then 1 when 306 then 1 when 307 then 2 when 308 then 3 when 309 then 3 when 331 then 1 when 332 then 3 when 333 then 2 else 4 end ,case spcode when 235 then '恒安嘉新' when 301 then '中新赛克' when 302 then '中新赛克' when 303 then '中新赛克' when 304 then '中新赛克' when 305 then '中新赛克' when 306 then '中新赛克' when 307 then '中新赛克' when 308 then '中新赛克' when 309 then '中新赛克' when 331 then '中新赛克' when 332 then '中新赛克' when 333 then '中新赛克' else '无' end,day from ibsdb.quality_result where day=data_format(current_timestamp - interval '1' day,'yyyyMMdd') and type = 'ybrz设备输出' order by type,spcode" | sed 's/"//g'>/tmp/base_device_output.csv

mysql -h10.10.120.1 -uroot -p111111 <<EOF
load data local infile  '/tmp/base_cdr_quality.csv' into table data_studio.base_cdr_quality fields terminated by ',';
load data local infile  '/tmp/base_ybrz_quality.csv' into table  data_studio.base_ybrz_quality fields terminated by ',';
load data local infile  '/tmp/base_station_quality.csv' into table  data_studio.base_station_quality fields terminated by ',';
load data local infile  '/tmp/base_device_output.csv' into table  data_studio.base_device_output fields terminated by ',';
EOF


