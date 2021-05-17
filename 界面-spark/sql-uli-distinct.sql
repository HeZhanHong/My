presto-cli-hive -e "insert into  ibsdb.t_cdr_k_uli select distinct c_uli, c_areacode, '', '', c_uli_addr,day,hour from original.t_cdr_k where day=data_format(current_timestamp - interval '1' day,'yyyyMMdd') and c_uli not like '460-00%' and c_ci < 268435455 "
presto-cli-hive -e "insert into  ibsdb.t_cdr_k_uli select distinct c_uli, c_areacode, '', '', c_uli_addr,data_format(current_timestamp - interval '1' day,'yyyyMMdd'),hour from original.t_cdr_k where day=data_format(current_timestamp - interval '1' day,'yyyyMMdd') and c_uli like '460-00%' and c_ci < 268435455;"
presto-cli-hive -e "insert into ibsdb.quality select 'uli_error' as type,c_spcode as spcode,count(distinct c_uli) as value,data_format(current_timestamp - interval '1' day,'yyyyMMdd') as day,hour from original.t_cdr_k  where day = data_format(current_timestamp - interval '1' day,'yyyyMMdd') and (c_uli not like '460%' or c_ci >= 268435455) and c_uli !='' group by c_spcode;"

presto-cli-hive -e "insert into ibsdb.quality select 'uli_common' as type,t_cdr_k.c_spcode as spcode,count(distinct substr(t_cdr_k.c_uli,7)) as value, data_format(current_timestamp - interval '1' day,'yyyyMMdd') as day,t_cdr_k.hour from (select distinct t_cdr_k.c_spcode,c_uli, data_format(current_timestamp - interval '1' day,'yyyyMMdd') as day ,t_cdr_k.hour from original.t_cdr_k where t_cdr_k.day=data_format(current_timestamp - interval '1' day,'yyyyMMdd') and t_cdr_k.c_uli like '460%' and c_lng = '' and c_lat = '' and c_areacode = ''  and c_ci < 268435455 ) t_cdr_k join ibsdb.t_dim_uli_info_full on substr(t_cdr_k.c_uli,7) = substr(t_dim_uli_info_full.c_uli,7) group by t_cdr_k.c_spcode;"



presto-cli-hive -e "insert into ibsdb.quality select 'uli_out_country' as type,1 as spcode,count(distinct c_uli) as value,data_format(current_timestamp - interval '1' day,'yyyyMMdd') as day  from (select tab.c_uli from (select distinct c_uli from (select * from ibsdb.t_cdr_k_uli where regexp_like(c_uli,'^[0-9-]*$')) tab333 where day = data_format(current_timestamp - interval '1' day,'yyyyMMdd') and c_uli like '460-00%' and c_lng = '' and c_lat = '' and c_uli_addr = '' and c_areacode = '' and cardinality(split(c_uli,'-'))=3 and (cast(substr(c_uli,8) as bigint)/256 < 16384 or cast(substr(c_uli,8) as bigint)/256 between 20480 and 32767 or cast(substr(c_uli,8) as bigint)/256 between 40704 and 143359 or cast(substr(c_uli,8) as bigint)/256 between 167936 and  327679 or cast(substr(c_uli,8) as bigint)/256 between 352256 and 417791 or cast(substr(c_uli,8) as bigint)/256 between 425788 and  438271 or cast(substr(c_uli,8) as bigint)/256 between 443392 and 446207 or cast(substr(c_uli,8) as bigint)/256 between 453120 and 626687 or cast(substr(c_uli,8) as bigint)/256 between 638976 and 655359 or cast(substr(c_uli,8) as bigint)/256 between 704512 and 851967 or cast(substr(c_uli,8) as bigint)/256 between 913408  and 987135 or cast(substr(c_uli,8) as bigint)/256 > 991231) )  tab left join (select distinct c_uli from ibsdb.t_cdr_k_uli where day = data_format(current_timestamp - interval '1' day,'yyyyMMdd') and c_uli like '460-00%' and c_lng = '' and c_lat = '' and c_uli_addr = '' and c_areacode = '' and cardinality(split(c_uli,'-'))=3 and substr(c_uli,7) in (select substr(c_uli,7) from ibsdb.t_dim_uli_info_full)) tb on tab.c_uli = tb.c_uli where tb.c_uli is null) tac"



presto-cli-hive -e "insert into ibsdb.quality select 'uli_out_country' as type,2 as spcode,count(distinct c_uli) as value,data_format(current_timestamp - interval '1' day,'yyyyMMdd') as day  from (select tab.c_uli from (select distinct c_uli from ibsdb.t_cdr_k_uli where day = data_format(current_timestamp - interval '1' day,'yyyyMMdd') and c_uli like '460-01%' and c_lng = '' and c_lat = '' and c_uli_addr = '' and c_areacode = '' and cardinality(split(c_uli,'-'))=3 and (cast(substr(c_uli,8) as bigint)/256 < 327680 or cast(substr(c_uli,8) as bigint)/256 between 364545 and 479231 or cast(substr(c_uli,8) as bigint)/256 between 548865 and 851967 or cast(substr(c_uli,8) as bigint)/256 > 864256 ) )  tab left join (select distinct c_uli from ibsdb.t_cdr_k_uli where day = data_format(current_timestamp - interval '1' day,'yyyyMMdd') and c_uli like '460-01%' and c_lng = '' and c_lat = '' and c_uli_addr = '' and c_areacode = '' and cardinality(split(c_uli,'-'))=3 and substr(c_uli,7) in (select substr(c_uli,7) from ibsdb.t_dim_uli_info_full)) tb on tab.c_uli = tb.c_uli where tb.c_uli is null) tac"


presto-cli-hive -e "insert into ibsdb.quality select 'uli_out_country' as type,3 as spcode,count(distinct c_uli) as value,data_format(current_timestamp - interval '1' day,'yyyyMMdd') as day  from (select tab.c_uli from (select distinct c_uli from ibsdb.t_cdr_k_uli where day = data_format(current_timestamp - interval '1' day,'yyyyMMdd') and c_uli like '460-03%' and c_lng = '' and c_lat = '' and c_uli_addr = '' and c_areacode = '' and cardinality(split(c_uli,'-'))=3 and (cast(substr(c_uli,8) as bigint)/256 < 237568 or cast(substr(c_uli,8) as bigint)/256 between 307180 and 479231 or cast(substr(c_uli,8) as bigint)/256 between 552958 and 851967 or cast(substr(c_uli,8) as bigint)/256 > 892927) )  tab left join (select distinct c_uli from ibsdb.t_cdr_k_uli where day = data_format(current_timestamp - interval '1' day,'yyyyMMdd') and c_uli like '460-03%' and c_lng = '' and c_lat = '' and c_uli_addr = '' and c_areacode = '' and cardinality(split(c_uli,'-'))=3 and substr(c_uli,7) in (select substr(c_uli,7) from ibsdb.t_dim_uli_info_full)) tb on tab.c_uli = tb.c_uli where tb.c_uli is null) tac"




