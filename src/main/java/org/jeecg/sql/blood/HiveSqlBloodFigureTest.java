package org.jeecg.sql.blood;

import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;
import org.jeecg.sql.blood.HiveSqlBloodGraphFactory;
import org.jeecg.sql.blood.HiveSqlBloodParserFactory;
import org.jeecg.sql.blood.model.*;
import org.jeecg.sql.blood.utils.GraphToTreeUtil;
import org.jeecg.sql.blood.vo.HIveSqlTableVertexTreeVo;
import org.jgrapht.graph.AsSubgraph;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HiveSqlBloodFigureTest {
	
	static String hqls0 = 
			"create table temp.b1(id string, name string) row format delimited fields terminated by ',';\n" + 
			"create table temp.b2(id string, age int) row format delimited fields terminated by ',';\n" + 
			"create table temp.c1(id string, name string) row format delimited fields terminated by ',';\n" + 
			"create table temp.c2(id string, age int) row format delimited fields terminated by ',';\n" + 
			"create table temp.d1(id string, name string, age int) row format delimited fields terminated by ',';\n" +
			"from temp.a1 insert into table temp.b1 select id, name insert into table temp.b2 select id, age;"+
			"insert overwrite table temp.c1 select id, name from temp.b1;" +
			"insert overwrite table temp.c2 select id, age from temp.b2;" +
			"insert overwrite table temp.d1 select t1.id, t1.name, t2.age from temp.c1 t1 join temp.c2 t2 on t1.id = t2.id;";
	
	
	static String hqls1 = "set mapreduce.job.queuename=root.ds;\r\n" + 
			"drop table if exists model.xh_lk_kb_increment_sample_0_1_0;\r\n" + 
			"create table model.xh_lk_kb_increment_sample_0_1_0 as select t1.apply_risk_id, t1.apply_risk_created_at, t1.apply_risk_result, t1.apply_risk_type, t5.apply_risk_id post_rid, t5.apply_risk_apply_id post_aid, t6.apply_status, t1.apply_risk_score, t6.apply_product_id, t6.apply_code, t6.apply_amount, t3.metadata_business_user_info_apply_times apply_times, t3.metadata_business_user_info_reg_time reg_time, t3.metadata_business_user_info_age age, t3.metadata_business_user_info_gender gender, substr(t7.individual_identity, 1, 2) prov, substr(t7.individual_identity, 1, 4) city, t7.individual_name, t7.individual_mobile, t7.individual_identity, t8.oauth_user_id, row_number() over(partition by t1.apply_risk_id order by t5.apply_risk_created_at desc) rank from ( select case when t2.recycle_quota_log_parent_id is null then t1.apply_risk_id else recycle_quota_log_parent_id end apply_risk_id from riskdata.o_apply_risk t1 left join ods.ods_r_recycle_quota_log t2 on t1.apply_risk_id = t2.recycle_quota_log_child_id where t1.apply_risk_source in (19, 29) and t1.apply_risk_type in (2, 5) and t1.apply_risk_created_at between date_sub(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'), ${parm_fou} ) and date_sub(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'), ${parm_tre} )) t0 left join riskdata.o_apply_risk t1 on t0.apply_risk_id = t1.apply_risk_id inner join riskdata.o_metadata_business_user_info t3 on t1.apply_risk_id = t3.metadata_business_user_info_apply_risk_id left join riskdata.o_apply_risk_binding t4 on t1.apply_risk_id = t4.apply_risk_binding_credit_id and t4.apply_risk_binding_main_id = 0 left join riskdata.o_apply_risk t5 on t4.apply_risk_binding_apply_risk_id = t5.apply_risk_id left join paydayloan.o_apply t6 on t5.apply_risk_apply_id = t6.apply_id left join paydayloan.o_oauth_user t8 on t6.apply_oauth_user_id = t8.oauth_user_id left join paydayloan.o_individual t7 on t8.oauth_user_individual_id = t7.individual_id where t3.metadata_business_user_info_apply_times > 1;\r\n" + 
			"drop table if exists model.xh_lk_kb_increment_sample_0;\r\n" + 
			"create table model.xh_lk_kb_increment_sample_0 as select apply_risk_id, apply_risk_created_at, apply_risk_result, apply_risk_type, post_rid, post_aid, apply_status, apply_risk_score, apply_product_id, apply_code, apply_amount, apply_times, reg_time, age, gender, prov, city, individual_name, individual_mobile, individual_identity, oauth_user_id from model.xh_lk_kb_increment_sample_0_1_0 where rank = 1;\r\n" + 
			"drop table if exists model.xh_lk_kb_increment_sample_label;\r\n" + 
			"create table model.xh_lk_kb_increment_sample_label as select t1.apply_risk_id, t1.post_rid, t1.post_aid, biz_report_time, biz_report_expect_at, biz_report_created_at, biz_report_status, datediff(if(instr(biz_report_time, '1000-01-01 00:00:00') > 0, from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss'), biz_report_time), biz_report_expect_at) overdue_days from model.xh_lk_kb_increment_sample_0 t1 left join riskdata.o_biz_report t2 on t1.post_rid = t2.biz_report_apply_risk_id and t2.biz_report_now_period = t2.biz_report_total_period and t2.biz_report_now_period = 1 and t2.biz_report_total_period = 1 and t2.biz_report_status SimpleSelectInLangDriverin (1, 2);\r\n" +
			"drop table if exists model.xh_lk_kb_increment_sample_1;\r\n" + 
			"create table model.xh_lk_kb_increment_sample_1 as select t1.apply_risk_id, t3.anti_risk_score ty2, t4.anti_risk_score ty2forqnn, t7.baidu_panshi_black_match, t7.baidu_panshi_black_score, t7.baidu_panshi_black_count_level1, t7.baidu_panshi_black_count_level2, t7.baidu_panshi_black_count_level3, t8.baidu_panshi_duotou_name_match, t8.baidu_panshi_duotou_name_score, t8.baidu_panshi_duotou_name_detail_key, t8.baidu_panshi_duotou_name_detail_val, t8.baidu_panshi_duotou_identity_match, t8.baidu_panshi_duotou_identity_score, t8.baidu_panshi_duotou_identity_detail_key, t8.baidu_panshi_duotou_identity_detail_val, t8.baidu_panshi_duotou_phone_match, t8.baidu_panshi_duotou_phone_score, t8.baidu_panshi_duotou_phone_detail_key, t8.baidu_panshi_duotou_phone_detail_val, t9.baidu_panshi_prea_models, t9.baidu_panshi_prea_score from model.xh_lk_kb_increment_sample_0 t1 left join riskdata.o_apply_risk_request t2 on t1.apply_risk_id = t2.apply_risk_request_apply_risk_id left join riskdata.o_anti_fraud t3 on t2.apply_risk_request_anti_fraud_version_two_point_zero = t3.anti_risk_request_id left join riskdata.o_anti_fraud t4 on t2.apply_risk_request_anti_fraud = t4.anti_risk_request_id left join riskdata.o_baidu_panshi_black t7 on t2.apply_risk_request_baidu_panshi_black = t7.baidu_panshi_black_risk_request_id and t7.baidu_panshi_black_match = 1 left join riskdata.o_baidu_panshi_duotou t8 on t2.apply_risk_request_baidu_panshi_duotou = t8.baidu_panshi_duotou_risk_request_id left join riskdata.o_baidu_panshi_prea t9 on t2.apply_risk_request_baidu_panshi_prea = t9.baidu_panshi_prea_risk_request_id;\r\n" + 
			"drop table if exists model.xh_lk_kb_increment_sample_3;\r\n" + 
			"create table model.xh_lk_kb_increment_sample_3 as select t1.apply_risk_id, t1.apply_risk_created_at created_at, t5.apply_order apply_order, t7.biz_report_time pre_finish_at, t6.apply_risk_created_at pre_created_at, row_number() over(partition by t1.apply_risk_id order by t6.apply_risk_created_at desc) rank, datediff(if(instr(t7.biz_report_time, '1000-01-01 00:00:00') > 0, from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss'), t7.biz_report_time), t7.biz_report_expect_at) yuqi_day, t7.biz_report_total_period pre_total_period from model.xh_lk_kb_increment_sample_0 t1 join paydayloan.o_apply t4 on t1.post_aid = t4.apply_id join paydayloan.o_apply t5 on t4.apply_oauth_user_id = t5.apply_oauth_user_id and t5.apply_product_id <> 44 join riskdata.o_apply_risk t6 on t5.apply_id = t6.apply_risk_apply_id and t6.apply_risk_source = 2 join riskdata.o_biz_report t7 on t6.apply_risk_id = t7.biz_report_apply_risk_id and t7.biz_report_status = 1 and t7.biz_report_now_period = t7.biz_report_total_period where t1.apply_risk_created_at > t6.apply_risk_created_at;\r\n" + 
			"drop table if exists model.xh_lk_kb_increment_sample_4;\r\n" + 
			"create table model.xh_lk_kb_increment_sample_4 as select apply_risk_id, max(yuqi_day) max_yuqi_day, min(yuqi_day) min_yuqi_day, avg(yuqi_day) mean_yuqi_day, sum(case when rank=1 then yuqi_day else 0 end) latest_yuqi_day, sum(case when apply_order=1 then yuqi_day else 0 end) farest_yuqi_day, sum(case when pre_total_period=1 then 1 else 0 end) danqi_num, sum(case when pre_total_period>1 then 1 else 0 end) duoqi_num, datediff(max(case when rank=1 then pre_created_at else 0 end), max(case when rank=2 then pre_finish_at else 0 end)) latest_borrow_span, datediff(max(case when rank=1 then pre_created_at else 0 end), max(case when apply_order=1 then pre_created_at else 0 end)) farest_created_span, datediff(max(case when rank=1 then pre_created_at else 0 end), max(case when apply_order=1 then pre_created_at else 0 end))/max(rank) mean_created_span from model.xh_lk_kb_increment_sample_3 group by apply_risk_id;\r\n" + 
			"drop table if exists model.xh_lk_kb_increment_sample_final;\r\n" + 
			"create table model.xh_lk_kb_increment_sample_final as select t1.apply_risk_id, t1.post_rid, t1.post_aid, t1.overdue_days, t1.biz_report_time, t1.biz_report_expect_at, t1.biz_report_created_at, t1.biz_report_status, t3.apply_risk_created_at, t3.apply_risk_result, t3.apply_status, t3.apply_risk_type, t2.ty2, t2.ty2forqnn, t2.baidu_panshi_black_match, t2.baidu_panshi_black_score, t2.baidu_panshi_black_count_level1, t2.baidu_panshi_black_count_level2, t2.baidu_panshi_black_count_level3, t2.baidu_panshi_duotou_name_match, t2.baidu_panshi_duotou_name_score, t2.baidu_panshi_duotou_name_detail_key, t2.baidu_panshi_duotou_name_detail_val, t2.baidu_panshi_duotou_identity_match, t2.baidu_panshi_duotou_identity_score, t2.baidu_panshi_duotou_identity_detail_key, t2.baidu_panshi_duotou_identity_detail_val, t2.baidu_panshi_duotou_phone_match, t2.baidu_panshi_duotou_phone_score, t2.baidu_panshi_duotou_phone_detail_key, t2.baidu_panshi_duotou_phone_detail_val, t2.baidu_panshi_prea_models, t2.baidu_panshi_prea_score, t3.apply_times, t3.age, t3.gender, t3.prov, t3.city, t3.individual_name, t3.individual_mobile, t3.individual_identity, t3.oauth_user_id, t3.apply_risk_score, t3.apply_product_id, t3.apply_code, t3.apply_amount, t4.max_yuqi_day, t4.min_yuqi_day, t4.mean_yuqi_day, t4.latest_yuqi_day, t4.farest_yuqi_day, t4.danqi_num, t4.duoqi_num, t4.latest_borrow_span, t4.farest_created_span, t4.mean_created_span from model.xh_lk_kb_increment_sample_label t1 left join model.xh_lk_kb_increment_sample_1 t2 on t1.apply_risk_id = t2.apply_risk_id left join model.xh_lk_kb_increment_sample_0 t3 on t1.apply_risk_id = t3.apply_risk_id left join model.xh_lk_kb_increment_sample_4 t4 on t1.apply_risk_id = t4.apply_risk_id;\r\n" + 
			"drop table if exists model.xh_lk_kb_update_temp;\r\n" + 
			"create table model.xh_lk_kb_update_temp as select * from ( select t2.apply_risk_id, t2.post_rid, t2.post_aid, datediff(if(instr(t1.biz_report_time, '1000-01-01 00:00:00') > 0, from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss'), t1.biz_report_time), t1.biz_report_expect_at) overdue_days, t1.biz_report_time, t1.biz_report_expect_at, t1.biz_report_created_at, t1.biz_report_status, t2.apply_risk_created_at, t2.apply_risk_result, t2.apply_status, t2.apply_risk_type, t2.ty2, t2.ty2forqnn, t2.baidu_panshi_black_match, t2.baidu_panshi_black_score, t2.baidu_panshi_black_count_level1, t2.baidu_panshi_black_count_level2, t2.baidu_panshi_black_count_level3, t2.baidu_panshi_duotou_name_match, t2.baidu_panshi_duotou_name_score, t2.baidu_panshi_duotou_name_detail_key, t2.baidu_panshi_duotou_name_detail_val, t2.baidu_panshi_duotou_identity_match, t2.baidu_panshi_duotou_identity_score, t2.baidu_panshi_duotou_identity_detail_key, t2.baidu_panshi_duotou_identity_detail_val, t2.baidu_panshi_duotou_phone_match, t2.baidu_panshi_duotou_phone_score, t2.baidu_panshi_duotou_phone_detail_key, t2.baidu_panshi_duotou_phone_detail_val, t2.baidu_panshi_prea_models, t2.baidu_panshi_prea_score, t2.apply_times, t2.age, t2.gender, t2.prov, t2.city, t2.individual_name, t2.individual_mobile, t2.individual_identity, t2.oauth_user_id, t2.apply_risk_score, t2.apply_product_id, t2.apply_code, t2.apply_amount, t2.max_yuqi_day, t2.min_yuqi_day, t2.mean_yuqi_day, t2.latest_yuqi_day, t2.farest_yuqi_day, t2.danqi_num, t2.duoqi_num, t2.latest_borrow_span, t2.farest_created_span, t2.mean_created_span from ( select * from model.xh_lk_kb_update where biz_report_status = 2 and dt = date_sub(from_unixtime(unix_timestamp()), 1)) t2 left join riskdata.o_biz_report t1 on t2.post_rid = t1.biz_report_apply_risk_id and t1.biz_report_total_period = 1 and t1.biz_report_status in (1, 2) union all select t2.apply_risk_id, t2.post_rid, t2.post_aid, overdue_days, t2.biz_report_time, t2.biz_report_expect_at, t2.biz_report_created_at, t2.biz_report_status, t2.apply_risk_created_at, t2.apply_risk_result, t2.apply_status, t2.apply_risk_type, t2.ty2, t2.ty2forqnn, t2.baidu_panshi_black_match, t2.baidu_panshi_black_score, t2.baidu_panshi_black_count_level1, t2.baidu_panshi_black_count_level2, t2.baidu_panshi_black_count_level3, t2.baidu_panshi_duotou_name_match, t2.baidu_panshi_duotou_name_score, t2.baidu_panshi_duotou_name_detail_key, t2.baidu_panshi_duotou_name_detail_val, t2.baidu_panshi_duotou_identity_match, t2.baidu_panshi_duotou_identity_score, t2.baidu_panshi_duotou_identity_detail_key, t2.baidu_panshi_duotou_identity_detail_val, t2.baidu_panshi_duotou_phone_match, t2.baidu_panshi_duotou_phone_score, t2.baidu_panshi_duotou_phone_detail_key, t2.baidu_panshi_duotou_phone_detail_val, t2.baidu_panshi_prea_models, t2.baidu_panshi_prea_score, t2.apply_times, t2.age, t2.gender, t2.prov, t2.city, t2.individual_name, t2.individual_mobile, t2.individual_identity, t2.oauth_user_id, t2.apply_risk_score, t2.apply_product_id, t2.apply_code, t2.apply_amount, t2.max_yuqi_day, t2.min_yuqi_day, t2.mean_yuqi_day, t2.latest_yuqi_day, t2.farest_yuqi_day, t2.danqi_num, t2.duoqi_num, t2.latest_borrow_span, t2.farest_created_span, t2.mean_created_span from model.xh_lk_kb_update t2 where (biz_report_status <> 2 or biz_report_status is null) and dt = date_sub(from_unixtime(unix_timestamp()), 1) union all select * from model.xh_lk_kb_increment_sample_final) t;\r\n" + 
			"insert overwrite table model.xh_lk_kb_update partition (dt= \"${parm_one}\") select * from model.xh_lk_kb_update_temp;\r\n" + 
			"alter table model.xh_lk_kb_update drop IF EXISTS partition (dt = \"${parm_two}\");\r\n" + 
			"insert overwrite table model.xh_lk_kb_features partition (dt = \"${parm_one}\") select t2.apply_risk_id, overdue_days, t2.biz_report_expect_at, t2.ty2, t2.ty2forqnn, t2.baidu_panshi_black_match, t2.baidu_panshi_black_score, t2.baidu_panshi_black_count_level1, t2.baidu_panshi_black_count_level2, t2.baidu_panshi_black_count_level3, t2.baidu_panshi_duotou_name_match, t2.baidu_panshi_duotou_name_score, t2.baidu_panshi_duotou_name_detail_key, t2.baidu_panshi_duotou_name_detail_val, t2.baidu_panshi_duotou_identity_match, t2.baidu_panshi_duotou_identity_score, t2.baidu_panshi_duotou_identity_detail_key, t2.baidu_panshi_duotou_identity_detail_val, t2.baidu_panshi_duotou_phone_match, t2.baidu_panshi_duotou_phone_score, t2.baidu_panshi_duotou_phone_detail_key, t2.baidu_panshi_duotou_phone_detail_val, t2.baidu_panshi_prea_models, t2.baidu_panshi_prea_score, t2.apply_times, t2.age, t2.gender, t2.prov, t2.city, t2.max_yuqi_day, t2.min_yuqi_day, t2.mean_yuqi_day, t2.latest_yuqi_day, t2.farest_yuqi_day, t2.danqi_num, t2.duoqi_num, t2.latest_borrow_span, t2.farest_created_span, t2.mean_created_span from model.xh_lk_kb_update_temp t2;\r\n" + 
			"alter table model.xh_lk_kb_features drop IF EXISTS partition (dt = \"${parm_two}\");\r\n" + 
			"insert overwrite table model.xh_lk_kb_features_total partition (dt = \"${parm_yesterday}\") select t2.apply_risk_id,collect_set(overdue_days)[0],collect_set(t2.biz_report_expect_at)[0],collect_set(t2.ty2)[0],collect_set(t2.ty2forqnn)[0],collect_set(t2.baidu_panshi_black_match)[0],collect_set(t2.baidu_panshi_black_score)[0],collect_set(t2.baidu_panshi_black_count_level1)[0],collect_set(t2.baidu_panshi_black_count_level2)[0],collect_set(t2.baidu_panshi_black_count_level3)[0],collect_set(t2.baidu_panshi_duotou_name_match)[0],collect_set(t2.baidu_panshi_duotou_name_score)[0],collect_set(t2.baidu_panshi_duotou_name_detail_key)[0],collect_set(t2.baidu_panshi_duotou_name_detail_val)[0],collect_set(t2.baidu_panshi_duotou_identity_match)[0],collect_set(t2.baidu_panshi_duotou_identity_score)[0],collect_set(t2.baidu_panshi_duotou_identity_detail_key)[0],collect_set(t2.baidu_panshi_duotou_identity_detail_val)[0],collect_set(t2.baidu_panshi_duotou_phone_match)[0],collect_set(t2.baidu_panshi_duotou_phone_score)[0],collect_set(t2.baidu_panshi_duotou_phone_detail_key)[0], collect_set(t2.baidu_panshi_duotou_phone_detail_val)[0],collect_set(t2.baidu_panshi_prea_models)[0],collect_set(t2.baidu_panshi_prea_score)[0],collect_set(t2.apply_times)[0],collect_set(t2.age)[0],collect_set(t2.gender)[0],collect_set(t2.prov)[0],collect_set(t2.city)[0],collect_set(t2.max_yuqi_day)[0],collect_set(t2.min_yuqi_day)[0],collect_set(t2.mean_yuqi_day)[0],collect_set(t2.latest_yuqi_day)[0],collect_set(t2.farest_yuqi_day)[0],collect_set(t2.danqi_num)[0],collect_set(t2.duoqi_num)[0],collect_set(t2.latest_borrow_span)[0],collect_set(t2.farest_created_span)[0],collect_set(t2.mean_created_span)[0] from model.xh_lk_kb_features t2 where dt = date_sub(from_unixtime(unix_timestamp()), 1) group by t2.apply_risk_id;";
	
	static String hqls2 = "set mapreduce.job.queuename=root.ds;\r\n" + 
			"   drop table if exists model.dm_regular_customer_biz_wt_dm_temp;\r\n" + 
			"   create table model.dm_regular_customer_biz_wt_dm_temp\r\n" + 
			"   stored as orc\r\n" + 
			"   as\r\n" + 
			"   select t1.apply_risk_id\r\n" + 
			"          ,substr(t1.apply_risk_created_at,1,19) apply_risk_created_at\r\n" + 
			"          ,substr(t1.apply_risk_created_at,1,19) apply_time_commit_at\r\n" + 
			"          ,t3.first_expect_at biz_report_expect_at\r\n" + 
			"          ,t3.first_main_report_time biz_report_time\r\n" + 
			"          ,t3.main_datediff overdue_days\r\n" + 
			"          ,t4.metadata_business_user_info_mobile individual_mobile\r\n" + 
			"          ,t4.metadata_business_user_info_age age\r\n" + 
			"          ,t6.individual_identity\r\n" + 
			"          ,t2.max_yuqi_day \r\n" + 
			"          ,t2.min_yuqi_day \r\n" + 
			"          ,t2.mean_yuqi_day \r\n" + 
			"          ,t2.std_yuqi_day \r\n" + 
			"          ,t2.last_latest_created_span \r\n" + 
			"          ,t2.first_last_created_span \r\n" + 
			"          ,t2.mean_first_last_created_span \r\n" + 
			"          ,t2.latest_yuqi_day \r\n" + 
			"          ,t2.farest_yuqi_day \r\n" + 
			"          ,t2.danqi_num \r\n" + 
			"          ,t2.duoqi_num \r\n" + 
			"          ,t2.latest_borrow_span \r\n" + 
			"          ,t2.farest_created_span \r\n" + 
			"          ,t2.mean_created_span \r\n" + 
			"          ,t2.baidu_panshi_prea_score \r\n" + 
			"          ,t2.baidu_panshi_duotou_name_score \r\n" + 
			"          ,t2.baidu_panshi_duotou_identity_score \r\n" + 
			"          ,t2.baidu_panshi_duotou_phone_score \r\n" + 
			"          ,t2.baidu_panshi_black_score \r\n" + 
			"          ,t2.baidu_panshi_black_count_level1 \r\n" + 
			"          ,t2.baidu_panshi_black_count_level2 \r\n" + 
			"          ,t2.baidu_panshi_black_count_level3                \r\n" + 
			"          ,t2.fbi_score \r\n" + 
			"          ,t2.fbi_desc \r\n" + 
			"          ,t2.xinyan_score \r\n" + 
			"          ,t2.anti_fraud_old \r\n" + 
			"          ,t2.anti_fraud \r\n" + 
			"          ,t2.anti_fraud_version_two_point_zero \r\n" + 
			"          ,t2.xcloud_score \r\n" + 
			"          ,t2.jd_ss_score_payday_sort_score \r\n" + 
			"          ,t2.td_score_final_score \r\n" + 
			"          ,t2.umeng_credit_score_credit_score \r\n" + 
			"          ,t2.max_call_in_len \r\n" + 
			"          ,t2.avg_call_in_len\r\n" + 
			"          ,t2.sum_call_in_len \r\n" + 
			"          ,t2.std_call_in_len \r\n" + 
			"          ,t2.max_call_cnt \r\n" + 
			"          ,t2.avg_call_cnt \r\n" + 
			"          ,t2.sum_call_cnt \r\n" + 
			"          ,t2.std_call_cnt \r\n" + 
			"          ,t2.max_call_len \r\n" + 
			"          ,t2.avg_call_len \r\n" + 
			"          ,t2.sum_call_len \r\n" + 
			"          ,t2.std_call_len \r\n" + 
			"          ,t2.max_call_out_cnt \r\n" + 
			"          ,t2.avg_call_out_cnt \r\n" + 
			"          ,t2.sum_call_out_cnt \r\n" + 
			"          ,t2.std_call_out_cnt \r\n" + 
			"          ,t2.max_call_out_len \r\n" + 
			"          ,t2.avg_call_out_len \r\n" + 
			"          ,t2.sum_call_out_len \r\n" + 
			"          ,t2.std_call_out_len              \r\n" + 
			"          ,t2.max_call_in_cnt \r\n" + 
			"          ,t2.avg_call_in_cnt \r\n" + 
			"          ,t2.sum_call_in_cnt \r\n" + 
			"          ,t2.std_call_in_cnt  \r\n" + 
			"          ,t2.max_1w \r\n" + 
			"          ,t2.avg_1w \r\n" + 
			"          ,t2.sum_1w \r\n" + 
			"          ,t2.std_1w \r\n" + 
			"          ,t2.count_1m \r\n" + 
			"          ,t2.max_1m \r\n" + 
			"          ,t2.avg_1m \r\n" + 
			"          ,t2.sum_1m \r\n" + 
			"          ,t2.std_1m \r\n" + 
			"          ,t2.max_3m \r\n" + 
			"          ,t2.avg_3m \r\n" + 
			"          ,t2.sum_3m \r\n" + 
			"          ,t2.std_3m \r\n" + 
			"          ,t2.max_early_morning \r\n" + 
			"          ,t2.avg_early_morning \r\n" + 
			"          ,t2.sum_early_morning \r\n" + 
			"          ,t2.std_early_morning \r\n" + 
			"          ,t2.max_morning \r\n" + 
			"          ,t2.avg_morning \r\n" + 
			"          ,t2.sum_morning \r\n" + 
			"          ,t2.std_morning    \r\n" + 
			"          ,t2.max_noon \r\n" + 
			"          ,t2.avg_noon \r\n" + 
			"          ,t2.sum_noon \r\n" + 
			"          ,t2.std_noon    \r\n" + 
			"          ,t2.max_afternoon \r\n" + 
			"          ,t2.avg_afternoon \r\n" + 
			"          ,t2.sum_afternoon \r\n" + 
			"          ,t2.std_afternoon    \r\n" + 
			"          ,t2.max_night \r\n" + 
			"          ,t2.avg_night \r\n" + 
			"          ,t2.sum_night \r\n" + 
			"          ,t2.std_night \r\n" + 
			"          ,t2.sum_all_day \r\n" + 
			"          ,t2.max_weekday \r\n" + 
			"          ,t2.avg_weekday \r\n" + 
			"          ,t2.sum_weekday \r\n" + 
			"          ,t2.std_weekday \r\n" + 
			"          ,t2.max_weekend \r\n" + 
			"          ,t2.avg_weekend \r\n" + 
			"          ,t2.sum_weekend \r\n" + 
			"          ,t2.std_weekend\r\n" + 
			"          ,t2.max_holiday \r\n" + 
			"          ,t2.avg_holiday \r\n" + 
			"          ,t2.sum_holiday \r\n" + 
			"          ,t2.std_holiday    \r\n" + 
			"          ,t2.call_cnt_gre_5\r\n" + 
			"          ,t2.call_cnt_equal_1 \r\n" + 
			"          ,t2.call_cnt_gre_5_ratio\r\n" + 
			"          ,t2.call_cnt_equal_1_ratio\r\n" + 
			"          ,t2.call_len_eql_gre_1 \r\n" + 
			"          ,t2.call_len_les_1 \r\n" + 
			"          ,t2.coc_gre_0 \r\n" + 
			"          ,t2.coc_equal_0 \r\n" + 
			"          ,t2.coc_gre_0_ratio \r\n" + 
			"          ,t2.cic_gre_0 \r\n" + 
			"          ,t2.cic_equal_0 \r\n" + 
			"          ,t2.cic_gre_0_ratio \r\n" + 
			"          ,t2.c1w_gre_0 \r\n" + 
			"          ,t2.c1w_equal_0 \r\n" + 
			"          ,t2.c1w_gre_0_ratio \r\n" + 
			"          ,t2.c1m_gre_0 \r\n" + 
			"          ,t2.c1m_equal_0 \r\n" + 
			"          ,t2.c1m_gre_0_ratio \r\n" + 
			"          ,t2.c3m_gre_0 \r\n" + 
			"          ,t2.c3m_equal_0 \r\n" + 
			"          ,t2.c3m_gre_0_ratio                \r\n" + 
			"          ,t2.ccem_gre_0 \r\n" + 
			"          ,t2.ccem_equal_0 \r\n" + 
			"          ,t2.ccem_gre_0_ratio                \r\n" + 
			"          ,t2.ccm_gre_0 \r\n" + 
			"          ,t2.ccm_equal_0 \r\n" + 
			"          ,t2.ccm_gre_0_ratio                  \r\n" + 
			"          ,t2.ccn_gre_0 \r\n" + 
			"          ,t2.ccn_equal_0 \r\n" + 
			"          ,t2.ccn_gre_0_ratio  \r\n" + 
			"          ,t2.ca_gre_0 \r\n" + 
			"          ,t2.ca_equal_0 \r\n" + 
			"          ,t2.ca_gre_0_ratio                 \r\n" + 
			"          ,t2.cn_gre_0 \r\n" + 
			"          ,t2.cn_equal_0 \r\n" + 
			"          ,t2.cn_gre_0_ratio                \r\n" + 
			"          ,t2.ccwd_gre_0 \r\n" + 
			"          ,t2.ccwd_equal_0 \r\n" + 
			"          ,t2.ccwd_gre_0_ratio                 \r\n" + 
			"          ,t2.ccwe_gre_0 \r\n" + 
			"          ,t2.ccwe_equal_0 \r\n" + 
			"          ,t2.ccwe_gre_0_ratio   \r\n" + 
			"          ,t2.count_region \r\n" + 
			"          ,t2.max_avg_cit \r\n" + 
			"          ,t2.diffdate \r\n" + 
			"          ,t2.equipment_app_name \r\n" + 
			"          ,t2.call_len_gre_0_ratio call_len_then_equal_1_ratio\r\n" + 
			"          ,CASE WHEN t3.main_datediff>14 THEN 1 ELSE 0 END 14d\r\n" + 
			"          ,CASE WHEN t3.main_datediff>7 THEN 1 ELSE 0 END 7d\r\n" + 
			"          ,CASE WHEN t3.main_datediff>1 THEN 1 ELSE 0 END 1d \r\n" + 
			"          ,t3.dt\r\n" + 
			"     from (select *\r\n" + 
			"             from dm_sandbox.dm_yk_apply_wt_base\r\n" + 
			"            where apply_risk_type = 2\r\n" + 
			"              and apply_risk_source in (19,27,30)\r\n" + 
			"              and main_post_rid is not null) t1\r\n" + 
			"left join (select *\r\n" + 
			"             from dm_sandbox.dm_yk_biz_wt_base\r\n" + 
			"            where dt = from_unixtime(unix_timestamp(date_sub(current_date(),1),'yyyy-MM-dd'),'yyyyMMdd')) t3\r\n" + 
			"       on t1.apply_risk_id = t3.apply_risk_id\r\n" + 
			"left join dm_sandbox.dm_yk_regular_customer_wt t2\r\n" + 
			"       on t1.apply_risk_id = t2.apply_risk_id\r\n" + 
			"left join dwb.dwb_r_metadata_business_user_info t4\r\n" + 
			"       on t1.apply_risk_id = t4.metadata_business_user_info_apply_risk_id\r\n" + 
			"left join dwb.dwb_p_oauth_user t5\r\n" + 
			"       on t3.oauth_user_id = t5.oauth_user_id\r\n" + 
			"left join dwb.dwb_p_individual t6\r\n" + 
			"       on t5.oauth_user_individual_id = t6.individual_id\r\n" + 
			"    where t3.main_post_rid is not null\r\n" + 
			"      and t3.total_period = 1\r\n" + 
			"      and t3.first_main_status in (1,2)\r\n" + 
			"      and t4.metadata_business_user_info_age >= 23\r\n" + 
			"      and substr(t3.first_expect_at,1,10) < from_unixtime(unix_timestamp(date_sub(current_date(),2),'yyyy-MM-dd'),'yyyy-MM-dd');\r\n" + 
			"    \r\n" + 
			"    \r\n" + 
			"    drop table if exists model.wl_equipment_app_name_1;\r\n" + 
			"    create table model.wl_equipment_app_name_1\r\n" + 
			"    as\r\n" + 
			"    select apply_risk_id\r\n" + 
			"           ,equipment_app_name\r\n" + 
			"      from model.dm_regular_customer_biz_wt_dm_temp;\r\n" + 
			"      \r\n" + 
			"      \r\n" + 
			"      \r\n" + 
			"    drop table if exists model.wl_equipment_app_name_2;\r\n" + 
			"    create table model.wl_equipment_app_name_2\r\n" + 
			"    as\r\n" + 
			"    select apply_risk_id\r\n" + 
			"           ,concat('\"',equipment_app_name,'\"') equipment_app_name_1\r\n" + 
			"      from model.wl_equipment_app_name_1;\r\n" + 
			"\r\n" + 
			"    \r\n" + 
			"    drop table if exists model.dm_regular_customer_biz_wt_dm_temp_1;\r\n" + 
			"    create table model.dm_regular_customer_biz_wt_dm_temp_1\r\n" + 
			"    stored as orc\r\n" + 
			"    as\r\n" + 
			"    select apply_risk_id\r\n" + 
			"           ,apply_risk_created_at\r\n" + 
			"           ,apply_time_commit_at\r\n" + 
			"           ,biz_report_expect_at\r\n" + 
			"           ,biz_report_time\r\n" + 
			"           ,overdue_days\r\n" + 
			"           ,individual_identity\r\n" + 
			"           ,individual_mobile\r\n" + 
			"           ,max_yuqi_day\r\n" + 
			"           ,min_yuqi_day \r\n" + 
			"           ,mean_yuqi_day\r\n" + 
			"           ,std_yuqi_day\r\n" + 
			"           ,last_latest_created_span\r\n" + 
			"           ,first_last_created_span\r\n" + 
			"           ,mean_first_last_created_span\r\n" + 
			"           ,latest_yuqi_day\r\n" + 
			"           ,farest_yuqi_day\r\n" + 
			"           ,danqi_num\r\n" + 
			"           ,duoqi_num\r\n" + 
			"           ,latest_borrow_span\r\n" + 
			"           ,farest_created_span\r\n" + 
			"           ,mean_created_span\r\n" + 
			"           ,baidu_panshi_prea_score\r\n" + 
			"           ,baidu_panshi_duotou_name_score\r\n" + 
			"           ,baidu_panshi_duotou_identity_score\r\n" + 
			"           ,baidu_panshi_duotou_phone_score \r\n" + 
			"           ,baidu_panshi_black_score \r\n" + 
			"           ,baidu_panshi_black_count_level1 \r\n" + 
			"           ,baidu_panshi_black_count_level2 \r\n" + 
			"           ,baidu_panshi_black_count_level3 \r\n" + 
			"           ,fbi_score \r\n" + 
			"           ,fbi_desc \r\n" + 
			"           ,xinyan_score \r\n" + 
			"           ,anti_fraud_old \r\n" + 
			"           ,anti_fraud \r\n" + 
			"           ,anti_fraud_version_two_point_zero \r\n" + 
			"           ,xcloud_score \r\n" + 
			"           ,jd_ss_score_payday_sort_score \r\n" + 
			"           ,td_score_final_score \r\n" + 
			"           ,umeng_credit_score_credit_score \r\n" + 
			"           ,max_call_in_len \r\n" + 
			"           ,avg_call_in_len\r\n" + 
			"           ,sum_call_in_len \r\n" + 
			"           ,std_call_in_len \r\n" + 
			"           ,max_call_cnt \r\n" + 
			"           ,avg_call_cnt \r\n" + 
			"           ,sum_call_cnt \r\n" + 
			"           ,std_call_cnt \r\n" + 
			"           ,max_call_len \r\n" + 
			"           ,avg_call_len \r\n" + 
			"           ,sum_call_len \r\n" + 
			"           ,std_call_len \r\n" + 
			"           ,max_call_out_cnt \r\n" + 
			"           ,avg_call_out_cnt \r\n" + 
			"           ,sum_call_out_cnt \r\n" + 
			"           ,std_call_out_cnt \r\n" + 
			"           ,max_call_out_len \r\n" + 
			"           ,avg_call_out_len \r\n" + 
			"           ,sum_call_out_len \r\n" + 
			"           ,std_call_out_len              \r\n" + 
			"           ,max_call_in_cnt \r\n" + 
			"           ,avg_call_in_cnt \r\n" + 
			"           ,sum_call_in_cnt \r\n" + 
			"           ,std_call_in_cnt  \r\n" + 
			"           ,max_1w \r\n" + 
			"           ,avg_1w \r\n" + 
			"           ,sum_1w \r\n" + 
			"           ,std_1w \r\n" + 
			"           ,count_1m \r\n" + 
			"           ,max_1m \r\n" + 
			"           ,avg_1m \r\n" + 
			"           ,sum_1m \r\n" + 
			"           ,std_1m \r\n" + 
			"           ,max_3m \r\n" + 
			"           ,avg_3m \r\n" + 
			"           ,sum_3m \r\n" + 
			"           ,std_3m \r\n" + 
			"           ,max_early_morning \r\n" + 
			"           ,avg_early_morning \r\n" + 
			"           ,sum_early_morning \r\n" + 
			"           ,std_early_morning \r\n" + 
			"           ,max_morning \r\n" + 
			"           ,avg_morning \r\n" + 
			"           ,sum_morning \r\n" + 
			"           ,std_morning    \r\n" + 
			"           ,max_noon \r\n" + 
			"           ,avg_noon \r\n" + 
			"           ,sum_noon \r\n" + 
			"           ,std_noon    \r\n" + 
			"           ,max_afternoon \r\n" + 
			"           ,avg_afternoon \r\n" + 
			"           ,sum_afternoon \r\n" + 
			"           ,std_afternoon \r\n" + 
			"           ,max_night \r\n" + 
			"           ,avg_night \r\n" + 
			"           ,sum_night \r\n" + 
			"           ,std_night \r\n" + 
			"           ,sum_all_day \r\n" + 
			"           ,max_weekday \r\n" + 
			"           ,avg_weekday \r\n" + 
			"           ,sum_weekday \r\n" + 
			"           ,std_weekday \r\n" + 
			"           ,max_weekend \r\n" + 
			"           ,avg_weekend \r\n" + 
			"           ,sum_weekend \r\n" + 
			"           ,std_weekend\r\n" + 
			"           ,max_holiday \r\n" + 
			"           ,avg_holiday \r\n" + 
			"           ,sum_holiday \r\n" + 
			"           ,std_holiday    \r\n" + 
			"           ,call_cnt_gre_5\r\n" + 
			"           ,call_cnt_equal_1 \r\n" + 
			"           ,call_cnt_gre_5_ratio\r\n" + 
			"           ,call_cnt_equal_1_ratio\r\n" + 
			"           ,call_len_eql_gre_1 \r\n" + 
			"           ,call_len_les_1 \r\n" + 
			"           ,coc_gre_0 \r\n" + 
			"           ,coc_equal_0 \r\n" + 
			"           ,coc_gre_0_ratio \r\n" + 
			"           ,cic_gre_0 \r\n" + 
			"           ,cic_equal_0 \r\n" + 
			"           ,cic_gre_0_ratio \r\n" + 
			"           ,c1w_gre_0 \r\n" + 
			"           ,c1w_equal_0 \r\n" + 
			"           ,c1w_gre_0_ratio \r\n" + 
			"           ,c1m_gre_0 \r\n" + 
			"           ,c1m_equal_0 \r\n" + 
			"           ,c1m_gre_0_ratio \r\n" + 
			"           ,c3m_gre_0 \r\n" + 
			"           ,c3m_equal_0 \r\n" + 
			"           ,c3m_gre_0_ratio                \r\n" + 
			"           ,ccem_gre_0 \r\n" + 
			"           ,ccem_equal_0 \r\n" + 
			"           ,ccem_gre_0_ratio                \r\n" + 
			"           ,ccm_gre_0 \r\n" + 
			"           ,ccm_equal_0 \r\n" + 
			"           ,ccm_gre_0_ratio                  \r\n" + 
			"           ,ccn_gre_0 \r\n" + 
			"           ,ccn_equal_0 \r\n" + 
			"           ,ccn_gre_0_ratio  \r\n" + 
			"           ,ca_gre_0 \r\n" + 
			"           ,ca_equal_0 \r\n" + 
			"           ,ca_gre_0_ratio                 \r\n" + 
			"           ,cn_gre_0 \r\n" + 
			"           ,cn_equal_0 \r\n" + 
			"           ,cn_gre_0_ratio                \r\n" + 
			"           ,ccwd_gre_0 \r\n" + 
			"           ,ccwd_equal_0 \r\n" + 
			"           ,ccwd_gre_0_ratio                 \r\n" + 
			"           ,ccwe_gre_0 \r\n" + 
			"           ,ccwe_equal_0 \r\n" + 
			"           ,ccwe_gre_0_ratio   \r\n" + 
			"           ,count_region \r\n" + 
			"           ,max_avg_cit \r\n" + 
			"           ,diffdate \r\n" + 
			"           ,equipment_app_name\r\n" + 
			"           ,call_len_then_equal_1_ratio\r\n" + 
			"           ,14d\r\n" + 
			"           ,7d\r\n" + 
			"           ,1d\r\n" + 
			"           ,dt\r\n" + 
			"      from model.dm_regular_customer_biz_wt_dm_temp;\r\n" + 
			"    \r\n" + 
			"        \r\n" + 
			"    create table if not exists model.dm_regular_customer_biz_wt(\r\n" + 
			"           apply_risk_id string\r\n" + 
			"           ,apply_risk_created_at string\r\n" + 
			"           ,overdue_days string\r\n" + 
			"           ,max_yuqi_day string\r\n" + 
			"           ,min_yuqi_day string\r\n" + 
			"           ,mean_yuqi_day double\r\n" + 
			"           ,std_yuqi_day double\r\n" + 
			"           ,last_latest_created_span string\r\n" + 
			"           ,first_last_created_span string\r\n" + 
			"           ,mean_first_last_created_span string\r\n" + 
			"           ,latest_yuqi_day string\r\n" + 
			"           ,farest_yuqi_day string\r\n" + 
			"           ,danqi_num string\r\n" + 
			"           ,duoqi_num string\r\n" + 
			"           ,latest_borrow_span string\r\n" + 
			"           ,farest_created_span string\r\n" + 
			"           ,mean_created_span string\r\n" + 
			"           \r\n" + 
			"           ,baidu_panshi_prea_score string\r\n" + 
			"           ,baidu_panshi_duotou_name_score string\r\n" + 
			"           ,baidu_panshi_duotou_identity_score string\r\n" + 
			"           ,baidu_panshi_duotou_phone_score string\r\n" + 
			"           ,baidu_panshi_black_score string\r\n" + 
			"           ,baidu_panshi_black_count_level1 string\r\n" + 
			"           ,baidu_panshi_black_count_level2 string\r\n" + 
			"           ,baidu_panshi_black_count_level3 string\r\n" + 
			"           \r\n" + 
			"           ,fbi_score string\r\n" + 
			"           ,fbi_desc string\r\n" + 
			"           ,xinyan_score string\r\n" + 
			"           ,anti_fraud_old string\r\n" + 
			"           ,anti_fraud string\r\n" + 
			"           ,anti_fraud_version_two_point_zero string\r\n" + 
			"           ,xcloud_score string\r\n" + 
			"           ,jd_ss_score_payday_sort_score string\r\n" + 
			"           ,td_score_final_score string\r\n" + 
			"           ,umeng_credit_score_credit_score string\r\n" + 
			"           \r\n" + 
			"           ,max_call_in_len string\r\n" + 
			"           ,avg_call_in_len string\r\n" + 
			"           ,sum_call_in_len string\r\n" + 
			"           ,std_call_in_len string\r\n" + 
			"           \r\n" + 
			"           ,max_call_cnt string\r\n" + 
			"           ,avg_call_cnt string\r\n" + 
			"           ,sum_call_cnt string\r\n" + 
			"           ,std_call_cnt string\r\n" + 
			"           \r\n" + 
			"           ,max_call_len string\r\n" + 
			"           ,avg_call_len string\r\n" + 
			"           ,sum_call_len string\r\n" + 
			"           ,std_call_len string\r\n" + 
			"           \r\n" + 
			"           ,max_call_out_cnt string\r\n" + 
			"           ,avg_call_out_cnt string\r\n" + 
			"           ,sum_call_out_cnt string\r\n" + 
			"           ,std_call_out_cnt string\r\n" + 
			"           \r\n" + 
			"           ,max_call_out_len string\r\n" + 
			"           ,avg_call_out_len string\r\n" + 
			"           ,sum_call_out_len string\r\n" + 
			"           ,std_call_out_len string               \r\n" + 
			"           \r\n" + 
			"           ,max_call_in_cnt string\r\n" + 
			"           ,avg_call_in_cnt string\r\n" + 
			"           ,sum_call_in_cnt string\r\n" + 
			"           ,std_call_in_cnt string   \r\n" + 
			"           \r\n" + 
			"           ,max_1w string\r\n" + 
			"           ,avg_1w string\r\n" + 
			"           ,sum_1w string\r\n" + 
			"           ,std_1w string\r\n" + 
			"           \r\n" + 
			"           ,count_1m string\r\n" + 
			"           ,max_1m string\r\n" + 
			"           ,avg_1m string\r\n" + 
			"           ,sum_1m string\r\n" + 
			"           ,std_1m string\r\n" + 
			"           \r\n" + 
			"           ,max_3m string\r\n" + 
			"           ,avg_3m string\r\n" + 
			"           ,sum_3m string\r\n" + 
			"           ,std_3m string\r\n" + 
			"           \r\n" + 
			"           ,max_early_morning string\r\n" + 
			"           ,avg_early_morning string\r\n" + 
			"           ,sum_early_morning string\r\n" + 
			"           ,std_early_morning string\r\n" + 
			"           \r\n" + 
			"           ,max_morning string\r\n" + 
			"           ,avg_morning string\r\n" + 
			"           ,sum_morning string\r\n" + 
			"           ,std_morning string   \r\n" + 
			"           \r\n" + 
			"           ,max_noon string\r\n" + 
			"           ,avg_noon string\r\n" + 
			"           ,sum_noon string\r\n" + 
			"           ,std_noon string   \r\n" + 
			"           \r\n" + 
			"           ,max_afternoon string\r\n" + 
			"           ,avg_afternoon string\r\n" + 
			"           ,sum_afternoon string\r\n" + 
			"           ,std_afternoon string   \r\n" + 
			"    \r\n" + 
			"           ,max_night string\r\n" + 
			"           ,avg_night string\r\n" + 
			"           ,sum_night string\r\n" + 
			"           ,std_night string \r\n" + 
			"           \r\n" + 
			"           ,sum_all_day string\r\n" + 
			"           \r\n" + 
			"           ,max_weekday string\r\n" + 
			"           ,avg_weekday string\r\n" + 
			"           ,sum_weekday string\r\n" + 
			"           ,std_weekday string\r\n" + 
			"           \r\n" + 
			"           ,max_weekend string\r\n" + 
			"           ,avg_weekend string\r\n" + 
			"           ,sum_weekend string\r\n" + 
			"           ,std_weekend string\r\n" + 
			"           \r\n" + 
			"           ,max_holiday string\r\n" + 
			"           ,avg_holiday string\r\n" + 
			"           ,sum_holiday string\r\n" + 
			"           ,std_holiday string    \r\n" + 
			"           \r\n" + 
			"           ,call_cnt_gre_5 string\r\n" + 
			"           ,call_cnt_equal_1 string\r\n" + 
			"           ,call_cnt_gre_5_ratio string\r\n" + 
			"           ,call_cnt_equal_1_ratio string\r\n" + 
			"           ,call_len_eql_gre_1 string\r\n" + 
			"           ,call_len_les_1 string\r\n" + 
			"\r\n" + 
			"           ,coc_gre_0 string\r\n" + 
			"           ,coc_equal_0 string\r\n" + 
			"           ,coc_gre_0_ratio string\r\n" + 
			"           \r\n" + 
			"           ,cic_gre_0 string\r\n" + 
			"           ,cic_equal_0 string\r\n" + 
			"           ,cic_gre_0_ratio string\r\n" + 
			"           \r\n" + 
			"           ,c1w_gre_0 string\r\n" + 
			"           ,c1w_equal_0 string\r\n" + 
			"           ,c1w_gre_0_ratio string\r\n" + 
			"           \r\n" + 
			"           ,c1m_gre_0 string\r\n" + 
			"           ,c1m_equal_0 string\r\n" + 
			"           ,c1m_gre_0_ratio string\r\n" + 
			"           \r\n" + 
			"           ,c3m_gre_0 string\r\n" + 
			"           ,c3m_equal_0 string\r\n" + 
			"           ,c3m_gre_0_ratio string               \r\n" + 
			"           \r\n" + 
			"           ,ccem_gre_0 string\r\n" + 
			"           ,ccem_equal_0 string\r\n" + 
			"           ,ccem_gre_0_ratio string               \r\n" + 
			"           \r\n" + 
			"           ,ccm_gre_0 string\r\n" + 
			"           ,ccm_equal_0 string\r\n" + 
			"           ,ccm_gre_0_ratio string                 \r\n" + 
			"           \r\n" + 
			"           ,ccn_gre_0 string\r\n" + 
			"           ,ccn_equal_0 string\r\n" + 
			"           ,ccn_gre_0_ratio string \r\n" + 
			"           \r\n" + 
			"           ,ca_gre_0 string\r\n" + 
			"           ,ca_equal_0 string\r\n" + 
			"           ,ca_gre_0_ratio string                \r\n" + 
			"           \r\n" + 
			"           ,cn_gre_0 string\r\n" + 
			"           ,cn_equal_0 string\r\n" + 
			"           ,cn_gre_0_ratio string               \r\n" + 
			"           \r\n" + 
			"           ,ccwd_gre_0 string\r\n" + 
			"           ,ccwd_equal_0 string\r\n" + 
			"           ,ccwd_gre_0_ratio string                \r\n" + 
			"           \r\n" + 
			"           ,ccwe_gre_0 string\r\n" + 
			"           ,ccwe_equal_0 string\r\n" + 
			"           ,ccwe_gre_0_ratio string  \r\n" + 
			"           \r\n" + 
			"           ,count_region string\r\n" + 
			"           ,max_avg_cit string\r\n" + 
			"           ,diffdate string\r\n" + 
			"           ,equipment_app_name string\r\n" + 
			"           ,equipment_app_name_1 string\r\n" + 
			"           ,call_len_then_equal_1_ratio string\r\n" + 
			"\r\n" + 
			"           ,14d string\r\n" + 
			"           ,7d string\r\n" + 
			"           ,1d string\r\n" + 
			"           )\r\n" + 
			"partitioned by(dt string comment '分区日期');\r\n" + 
			"           \r\n" + 
			"           \r\n" + 
			"\r\n" + 
			"           \r\n" + 
			"           \r\n" + 
			"    SET hive.exec.dynamic.partition=true;\r\n" + 
			"    SET hive.exec.dynamic.partition.mode=nonstrict;\r\n" + 
			"    SET hive.exec.max.dynamic.partitions=100000;\r\n" + 
			"    SET hive.exec.max.dynamic.partitions.pernode=100000;\r\n" + 
			"    insert overwrite table model.dm_regular_customer_biz_wt partition (dt)\r\n" + 
			"    select t1.apply_risk_id \r\n" + 
			"           ,t1.apply_risk_created_at \r\n" + 
			"           ,t1.overdue_days \r\n" + 
			"           ,t1.max_yuqi_day \r\n" + 
			"           ,t1.min_yuqi_day \r\n" + 
			"           ,t1.mean_yuqi_day \r\n" + 
			"           ,t1.std_yuqi_day \r\n" + 
			"           ,t1.last_latest_created_span \r\n" + 
			"           ,t1.first_last_created_span \r\n" + 
			"           ,t1.mean_first_last_created_span \r\n" + 
			"           ,t1.latest_yuqi_day \r\n" + 
			"           ,t1.farest_yuqi_day \r\n" + 
			"           ,t1.danqi_num \r\n" + 
			"           ,t1.duoqi_num \r\n" + 
			"           ,t1.latest_borrow_span \r\n" + 
			"           ,t1.farest_created_span \r\n" + 
			"           ,t1.mean_created_span \r\n" + 
			"           ,t1.baidu_panshi_prea_score \r\n" + 
			"           ,t1.baidu_panshi_duotou_name_score \r\n" + 
			"           ,t1.baidu_panshi_duotou_identity_score \r\n" + 
			"           ,t1.baidu_panshi_duotou_phone_score \r\n" + 
			"           ,t1.baidu_panshi_black_score \r\n" + 
			"           ,t1.baidu_panshi_black_count_level1 \r\n" + 
			"           ,t1.baidu_panshi_black_count_level2 \r\n" + 
			"           ,t1.baidu_panshi_black_count_level3 \r\n" + 
			"           ,t1.fbi_score \r\n" + 
			"           ,t1.fbi_desc \r\n" + 
			"           ,t1.xinyan_score \r\n" + 
			"           ,t1.anti_fraud_old \r\n" + 
			"           ,t1.anti_fraud \r\n" + 
			"           ,t1.anti_fraud_version_two_point_zero \r\n" + 
			"           ,t1.xcloud_score \r\n" + 
			"           ,t1.jd_ss_score_payday_sort_score \r\n" + 
			"           ,t1.td_score_final_score \r\n" + 
			"           ,t1.umeng_credit_score_credit_score \r\n" + 
			"           ,t1.max_call_in_len \r\n" + 
			"           ,t1.avg_call_in_len\r\n" + 
			"           ,t1.sum_call_in_len \r\n" + 
			"           ,t1.std_call_in_len \r\n" + 
			"           ,t1.max_call_cnt \r\n" + 
			"           ,t1.avg_call_cnt \r\n" + 
			"           ,t1.sum_call_cnt \r\n" + 
			"           ,t1.std_call_cnt \r\n" + 
			"           ,t1.max_call_len \r\n" + 
			"           ,t1.avg_call_len \r\n" + 
			"           ,t1.sum_call_len \r\n" + 
			"           ,t1.std_call_len \r\n" + 
			"           ,t1.max_call_out_cnt \r\n" + 
			"           ,t1.avg_call_out_cnt \r\n" + 
			"           ,t1.sum_call_out_cnt \r\n" + 
			"           ,t1.std_call_out_cnt \r\n" + 
			"           ,t1.max_call_out_len \r\n" + 
			"           ,t1.avg_call_out_len \r\n" + 
			"           ,t1.sum_call_out_len \r\n" + 
			"           ,t1.std_call_out_len              \r\n" + 
			"           ,t1.max_call_in_cnt \r\n" + 
			"           ,t1.avg_call_in_cnt \r\n" + 
			"           ,t1.sum_call_in_cnt \r\n" + 
			"           ,t1.std_call_in_cnt  \r\n" + 
			"           ,t1.max_1w \r\n" + 
			"           ,t1.avg_1w \r\n" + 
			"           ,t1.sum_1w \r\n" + 
			"           ,t1.std_1w \r\n" + 
			"           ,t1.count_1m \r\n" + 
			"           ,t1.max_1m \r\n" + 
			"           ,t1.avg_1m \r\n" + 
			"           ,t1.sum_1m \r\n" + 
			"           ,t1.std_1m \r\n" + 
			"           ,t1.max_3m \r\n" + 
			"           ,t1.avg_3m \r\n" + 
			"           ,t1.sum_3m \r\n" + 
			"           ,t1.std_3m \r\n" + 
			"           ,t1.max_early_morning \r\n" + 
			"           ,t1.avg_early_morning \r\n" + 
			"           ,t1.sum_early_morning \r\n" + 
			"           ,t1.std_early_morning \r\n" + 
			"           ,t1.max_morning \r\n" + 
			"           ,t1.avg_morning \r\n" + 
			"           ,t1.sum_morning \r\n" + 
			"           ,t1.std_morning    \r\n" + 
			"           ,t1.max_noon \r\n" + 
			"           ,t1.avg_noon \r\n" + 
			"           ,t1.sum_noon \r\n" + 
			"           ,t1.std_noon    \r\n" + 
			"           ,t1.max_afternoon \r\n" + 
			"           ,t1.avg_afternoon \r\n" + 
			"           ,t1.sum_afternoon \r\n" + 
			"           ,t1.std_afternoon    \r\n" + 
			"           ,t1.max_night \r\n" + 
			"           ,t1.avg_night \r\n" + 
			"           ,t1.sum_night \r\n" + 
			"           ,t1.std_night \r\n" + 
			"           ,t1.sum_all_day \r\n" + 
			"           ,t1.max_weekday \r\n" + 
			"           ,t1.avg_weekday \r\n" + 
			"           ,t1.sum_weekday \r\n" + 
			"           ,t1.std_weekday \r\n" + 
			"           ,t1.max_weekend \r\n" + 
			"           ,t1.avg_weekend \r\n" + 
			"           ,t1.sum_weekend \r\n" + 
			"           ,t1.std_weekend\r\n" + 
			"           ,t1.max_holiday \r\n" + 
			"           ,t1.avg_holiday \r\n" + 
			"           ,t1.sum_holiday \r\n" + 
			"           ,t1.std_holiday    \r\n" + 
			"           ,t1.call_cnt_gre_5\r\n" + 
			"           ,t1.call_cnt_equal_1 \r\n" + 
			"           ,t1.call_cnt_gre_5_ratio\r\n" + 
			"           ,t1.call_cnt_equal_1_ratio\r\n" + 
			"           ,t1.call_len_eql_gre_1 \r\n" + 
			"           ,t1.call_len_les_1 \r\n" + 
			"           ,t1.coc_gre_0 \r\n" + 
			"           ,t1.coc_equal_0 \r\n" + 
			"           ,t1.coc_gre_0_ratio \r\n" + 
			"           ,t1.cic_gre_0 \r\n" + 
			"           ,t1.cic_equal_0 \r\n" + 
			"           ,t1.cic_gre_0_ratio \r\n" + 
			"           ,t1.c1w_gre_0 \r\n" + 
			"           ,t1.c1w_equal_0 \r\n" + 
			"           ,t1.c1w_gre_0_ratio \r\n" + 
			"           ,t1.c1m_gre_0 \r\n" + 
			"           ,t1.c1m_equal_0 \r\n" + 
			"           ,t1.c1m_gre_0_ratio \r\n" + 
			"           ,t1.c3m_gre_0 \r\n" + 
			"           ,t1.c3m_equal_0 \r\n" + 
			"           ,t1.c3m_gre_0_ratio                \r\n" + 
			"           ,t1.ccem_gre_0 \r\n" + 
			"           ,t1.ccem_equal_0 \r\n" + 
			"           ,t1.ccem_gre_0_ratio                \r\n" + 
			"           ,t1.ccm_gre_0 \r\n" + 
			"           ,t1.ccm_equal_0 \r\n" + 
			"           ,t1.ccm_gre_0_ratio                  \r\n" + 
			"           ,t1.ccn_gre_0 \r\n" + 
			"           ,t1.ccn_equal_0 \r\n" + 
			"           ,t1.ccn_gre_0_ratio  \r\n" + 
			"           ,t1.ca_gre_0 \r\n" + 
			"           ,t1.ca_equal_0 \r\n" + 
			"           ,t1.ca_gre_0_ratio                 \r\n" + 
			"           ,t1.cn_gre_0 \r\n" + 
			"           ,t1.cn_equal_0 \r\n" + 
			"           ,t1.cn_gre_0_ratio                \r\n" + 
			"           ,t1.ccwd_gre_0 \r\n" + 
			"           ,t1.ccwd_equal_0 \r\n" + 
			"           ,t1.ccwd_gre_0_ratio                 \r\n" + 
			"           ,t1.ccwe_gre_0 \r\n" + 
			"           ,t1.ccwe_equal_0 \r\n" + 
			"           ,t1.ccwe_gre_0_ratio   \r\n" + 
			"           ,t1.count_region \r\n" + 
			"           ,t1.max_avg_cit \r\n" + 
			"           ,t1.diffdate \r\n" + 
			"           ,t1.equipment_app_name\r\n" + 
			"           ,t2.equipment_app_name_1\r\n" + 
			"           ,t1.call_len_then_equal_1_ratio\r\n" + 
			"           ,t1.14d\r\n" + 
			"           ,t1.7d\r\n" + 
			"           ,t1.1d \r\n" + 
			"           ,t1.dt\r\n" + 
			"      from model.dm_regular_customer_biz_wt_dm_temp_1 t1\r\n" + 
			" left join model.wl_equipment_app_name_2 t2\r\n" + 
			"        on t1.apply_risk_id = t2.apply_risk_id;";


	public static void main(String[] args) throws Exception {
		List<SQLResult> results = HiveSqlBloodParserFactory.parser(hqls2);
		for(SQLResult item : results){
			System.out.println();
			System.out.println(item.getCurrentSql());
			System.out.println(item.getInputTables());
			System.out.println(item.getOutputTables());
			for(ColLine i :item.getColLineList()){
				System.out.println(i);
			}
		}
		String table = "model.dm_regular_customer_biz_wt";
		HIveSqlTableVertexTreeVo treeVo = new HIveSqlTableVertexTreeVo(table);
		HiveSqlBloodGraphFactory graph = HiveSqlBloodGraphFactory.create(results, true);
		Map<TableVertex, AsSubgraph<TableVertex, TableEdge>> subgraphs = graph.getTableFieldBloodChain(new TableVertex(TableLable.LABLE_TABLE, table), false);
		Set<HIveSqlTableVertexTreeVo> children = new HashSet<HIveSqlTableVertexTreeVo>();
		int i = 0;
		for(TableVertex vertex : subgraphs.keySet()) {
			System.out.println("-----------------------------------------");
			AsSubgraph<TableVertex, TableEdge> subgraph = subgraphs.get(vertex);
			if(i<=15) {
				children.add(GraphToTreeUtil.convert(vertex, subgraph));
			}
			i ++;
			System.out.println(new Gson().toJson(GraphToTreeUtil.convert(vertex, subgraph)));
//			GraphShowFactory.show(subgraphs);
		}
		treeVo.setChildren(children);
		System.out.println("-----------------------------------------");
		System.out.println("aaa:"+ new Gson().toJson(treeVo));
		
		FileUtils.write(new File("blood.json"), new Gson().toJson(treeVo));
		
//		GraphShowFactory.show(graph.getTableFieldBloodChain(new TableVertex(TableLable.LABLE_TABLE, "temp.d1"), false));
		
//		GraphToTreeUtil.convert(vertex, graph)
		
		
		
//		GraphShowFactory.show(graph.getCompleteBloodGraph(true, true, true));
		
//		Graph<TableVertex, TableEdge> graph = HiveSqlBloodFigureGraphFactory.toBloodFigureGraph(results, Level.FILELD, true);
//		Map<TableVertex, Graph<TableVertex, TableEdge>> subgraph = HiveSqlBloodFigureGraphFactory.fieldBloodChain(graph, "model.xh_lk_kb_features_total");
//		for(TableVertex vertex : subgraph.keySet()) {
//			HiveSqlBloodFigureGraphFactory.show(subgraph.get(vertex));
//		}
		
		
//		HiveSqlBloodFigureGraphFactory.show(graph.getTableBloodGraph(true));
//		HiveSqlBloodFigureGraphFactory.show(graph.getFieldBloodGraph(false));
//		HiveSqlBloodFigureGraphFactory.show(graph.getTableFieldBloodGraph(false));
//		HiveSqlBloodFigureGraphFactory.show(graph.getSingleFieldBloodChain(new TableVertex(TableLable.LABLE_FIELD, "temp.d1.name"), true));
//		HiveSqlBloodFigureGraphFactory.show(graph.getSingleFieldBloodChain(new TableVertex(TableLable.LABLE_FIELD, "temp.d1.name"), false));
		
//		GraphShowFactory.show(graph.getTableFieldBloodChain(new TableVertex(TableLable.LABLE_TABLE, "model.xh_lk_kb_features_total"), false));
		
//		HiveSqlBloodFigureGraphFactory.show(graph.getTableFieldBloodChain(new TableVertex(TableLable.LABLE_TABLE, "model.xh_lk_kb_increment_sample_0"), true));
//		HiveSqlBloodFigureGraphFactory.show(graph.getTableFieldBloodChain(new TableVertex(TableLable.LABLE_TABLE, "model.xh_lk_kb_increment_sample_0"), false));
		
		
//		Map<String, Object> elements =new HashMap<>();
//		List<Map<String, Object>> nodes = new ArrayList<>();
//		for(TableVertex vertex : graph.vertexSet()) {
//			Map<String, Object> data = new HashMap<>();
//			Map<String, Object> item = new HashMap<>();
//			item.put("id", vertex.getName());
//			item.put("target", false);
//			if(vertex.getName().indexOf(".") == vertex.getName().lastIndexOf(".")) {
//				item.put("type", "Company");
//			}
//			else {
//				item.put("type", "People");
//			}
//			data.put("data", item);
//			nodes.add(data);
//		}
//		elements.put("nodes", nodes);
//		
//		List<Map<String, Object>> edges = new ArrayList<>();
//		for(TableEdge edge : graph.edgeSet()) {
//			Map<String, Object> data = new HashMap<>();
//			Map<String, Object> item = new HashMap<>();
//			
//			
//			item.put("source", graph.getEdgeSource(edge).getName());
//			item.put("target", graph.getEdgeTarget(edge).getName());
//			item.put("label", edge.getRelation());
//			
//			data.put("data", item);
//			edges.add(data);
//		}
//		elements.put("edges", edges);
//		System.out.println(new Gson().toJson(elements));
		
		//显示图
		
//		graph.addEdge(new TableVertex("temp.d1"), new TableVertex("temp.d1.age"));
		
//		BreadthFirstIterator<TableVertex, TableEdge> bfi = new BreadthFirstIterator<>(graph, new TableVertex("temp.a1.id"));
		
		
//		bfi.
//		graph = bfi.getGraph()
		
//		while (bfi.hasNext()) {
//	            System.out.println( bfi.next() );
//	        }
//		 HiveSqlBloodFigureGraphFactory.show(bfi.getGraph());
		
		
	}

}
