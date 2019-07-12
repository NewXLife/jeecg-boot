package org.jeecg.sql.blood;

import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.jeecg.sql.blood.utils.HiveTableUtil;
import org.jeecg.sql.blood.vo.HiveSqlAnalyzeResult;

import java.io.IOException;

public class HiveTableUtilTest {

	public static void main(String[] args) throws SemanticException, ParseException, IOException {
		// TODO Auto-generated method stub
		String hql = "insert overwrite table model.temp_wudewen_sample_c1 select id, name, execute_id from model.temp_wudewen_sample_b1";
		
		String hql1= "set mapreduce.job.queuename=root.ds;\r\n" + 
				"       set hive.support.quoted.identifiers=None;\r\n" + 
				"       drop table if exists dm_temp.dm_regular_customer_biz_wt_sheng;\r\n" + 
				"       create table dm_temp.dm_regular_customer_biz_wt_sheng\r\n" + 
				"       as\r\n" + 
				"       select t1.apply_risk_id\r\n" + 
				"              ,substr(t1.apply_risk_created_at,1,19) apply_risk_created_at\r\n" + 
				"              ,t3.main_datediff overdue_days\r\n" + 
				"              ,t4.metadata_business_user_info_mobile mobile\r\n" + 
				"              ,t4.metadata_business_user_info_age age\r\n" + 
				"              ,t6.individual_identity\r\n" + 
				"              ,t2.`(apply_risk_created_at|apply_risk_id|dt|apply_times|apply_risk_type|call_len_gre_0_ratio)?+.+`\r\n" + 
				"              ,t2.call_len_gre_0_ratio call_len_then_equal_1_ratio\r\n" + 
				"         from (select *\r\n" + 
				"                 from dm_sandbox.dm_yk_apply_wt_base\r\n" + 
				"                where apply_risk_type =2\r\n" + 
				"                  and apply_risk_source in (19,27,30)\r\n" + 
				"                  and main_post_rid is not null) t1\r\n" + 
				"    left join (select *\r\n" + 
				"                 from dm_sandbox.dm_yk_biz_wt_base\r\n" + 
				"                where dt = from_unixtime(unix_timestamp(date_sub(current_date(),1),'yyyy-MM-dd'),'yyyyMMdd')) t3\r\n" + 
				"           on t1.apply_risk_id = t3.apply_risk_id\r\n" + 
				"    left join dm_sandbox.dm_yk_regular_customer_wt t2\r\n" + 
				"           on t1.apply_risk_id = t2.apply_risk_id\r\n" + 
				"    left join dwb.dwb_r_metadata_business_user_info t4\r\n" + 
				"          on t1.apply_risk_id = t4.metadata_business_user_info_apply_risk_id\r\n" + 
				"    left join dwb.dwb_p_oauth_user t5\r\n" + 
				"           on t3.oauth_user_id = t5.oauth_user_id\r\n" + 
				"    left join dwb.dwb_p_individual t6\r\n" + 
				"           on t5.oauth_user_individual_id = t6.individual_id\r\n" + 
				"        where t3.main_post_rid is not null\r\n" + 
				"          and t3.total_period = 1\r\n" + 
				"          and t3.first_main_status = 1\r\n" + 
				"          and t4.metadata_business_user_info_age >= 23;\r\n" + 
				"        \r\n" + 
				"        \r\n" + 
				"        \r\n" + 
				"        drop table if exists dm_temp.dm_regular_customer_biz_wt_1_sheng;\r\n" + 
				"        create table dm_temp.dm_regular_customer_biz_wt_1_sheng\r\n" + 
				"        as\r\n" + 
				"        select t.`(mobile|age|individual_identity|phone_0_num|phone_1_num|phone_4_num|phone_0_num_ratio|phone_1_num_ratio|phone_4_num_ratio|phone_short_num|phone_short_num_ratio)?+.+`\r\n" + 
				"          from dm_temp.dm_regular_customer_biz_wt_sheng t;\r\n" + 
				"        \r\n" + 
				"            \r\n" + 
				"            \r\n" + 
				"        drop table if exists dm_sandbox.dm_regular_customer_biz_wt_sheng;\r\n" + 
				"        create table dm_sandbox.dm_regular_customer_biz_wt_sheng(\r\n" + 
				"               apply_risk_id string\r\n" + 
				"               ,apply_risk_created_at string\r\n" + 
				"               ,overdue_days string\r\n" + 
				"               ,max_yuqi_day string\r\n" + 
				"               ,min_yuqi_day string\r\n" + 
				"               ,mean_yuqi_day double\r\n" + 
				"               ,std_yuqi_day double\r\n" + 
				"               ,last_latest_created_span string\r\n" + 
				"               ,first_last_created_span string\r\n" + 
				"               ,mean_first_last_created_span string\r\n" + 
				"               ,latest_yuqi_day string\r\n" + 
				"               ,farest_yuqi_day string\r\n" + 
				"               ,danqi_num string\r\n" + 
				"               ,duoqi_num string\r\n" + 
				"               ,latest_borrow_span string\r\n" + 
				"               ,farest_created_span string\r\n" + 
				"               ,mean_created_span string\r\n" + 
				"               \r\n" + 
				"               ,baidu_panshi_prea_score string\r\n" + 
				"               ,baidu_panshi_duotou_name_score string\r\n" + 
				"               ,baidu_panshi_duotou_identity_score string\r\n" + 
				"               ,baidu_panshi_duotou_phone_score string\r\n" + 
				"               ,baidu_panshi_black_score string\r\n" + 
				"               ,baidu_panshi_black_count_level1 string\r\n" + 
				"               ,baidu_panshi_black_count_level2 string\r\n" + 
				"               ,baidu_panshi_black_count_level3 string\r\n" + 
				"               \r\n" + 
				"               ,fbi_score string\r\n" + 
				"               ,fbi_desc string\r\n" + 
				"               ,xinyan_score string\r\n" + 
				"               ,anti_fraud_old string\r\n" + 
				"               ,anti_fraud string\r\n" + 
				"               ,anti_fraud_version_two_point_zero string\r\n" + 
				"               ,xcloud_score string\r\n" + 
				"               ,jd_ss_score_payday_sort_score string\r\n" + 
				"               ,td_score_final_score string\r\n" + 
				"               ,umeng_credit_score_credit_score string\r\n" + 
				"               \r\n" + 
				"               ,max_call_in_len string\r\n" + 
				"               ,avg_call_in_len string\r\n" + 
				"               ,sum_call_in_len string\r\n" + 
				"               ,std_call_in_len string\r\n" + 
				"               \r\n" + 
				"               ,max_call_cnt string\r\n" + 
				"               ,avg_call_cnt string\r\n" + 
				"               ,sum_call_cnt string\r\n" + 
				"               ,std_call_cnt string\r\n" + 
				"               \r\n" + 
				"               ,max_call_len string\r\n" + 
				"               ,avg_call_len string\r\n" + 
				"               ,sum_call_len string\r\n" + 
				"               ,std_call_len string\r\n" + 
				"               \r\n" + 
				"               ,max_call_out_cnt string\r\n" + 
				"               ,avg_call_out_cnt string\r\n" + 
				"               ,sum_call_out_cnt string\r\n" + 
				"               ,std_call_out_cnt string\r\n" + 
				"               \r\n" + 
				"               ,max_call_out_len string\r\n" + 
				"               ,avg_call_out_len string\r\n" + 
				"               ,sum_call_out_len string\r\n" + 
				"               ,std_call_out_len string               \r\n" + 
				"               \r\n" + 
				"               ,max_call_in_cnt string\r\n" + 
				"               ,avg_call_in_cnt string\r\n" + 
				"               ,sum_call_in_cnt string\r\n" + 
				"               ,std_call_in_cnt string   \r\n" + 
				"               \r\n" + 
				"               ,max_1w string\r\n" + 
				"               ,avg_1w string\r\n" + 
				"               ,sum_1w string\r\n" + 
				"               ,std_1w string\r\n" + 
				"               \r\n" + 
				"               ,count_1m string\r\n" + 
				"               ,max_1m string\r\n" + 
				"               ,avg_1m string\r\n" + 
				"               ,sum_1m string\r\n" + 
				"               ,std_1m string\r\n" + 
				"               \r\n" + 
				"               ,max_3m string\r\n" + 
				"               ,avg_3m string\r\n" + 
				"               ,sum_3m string\r\n" + 
				"               ,std_3m string\r\n" + 
				"               \r\n" + 
				"               ,max_early_morning string\r\n" + 
				"               ,avg_early_morning string\r\n" + 
				"               ,sum_early_morning string\r\n" + 
				"               ,std_early_morning string\r\n" + 
				"               \r\n" + 
				"               ,max_morning string\r\n" + 
				"               ,avg_morning string\r\n" + 
				"               ,sum_morning string\r\n" + 
				"               ,std_morning string   \r\n" + 
				"               \r\n" + 
				"               ,max_noon string\r\n" + 
				"               ,avg_noon string\r\n" + 
				"               ,sum_noon string\r\n" + 
				"               ,std_noon string   \r\n" + 
				"               \r\n" + 
				"               ,max_afternoon string\r\n" + 
				"               ,avg_afternoon string\r\n" + 
				"               ,sum_afternoon string\r\n" + 
				"               ,std_afternoon string   \r\n" + 
				"        \r\n" + 
				"               ,max_night string\r\n" + 
				"               ,avg_night string\r\n" + 
				"               ,sum_night string\r\n" + 
				"               ,std_night string \r\n" + 
				"               \r\n" + 
				"               ,sum_all_day string\r\n" + 
				"               \r\n" + 
				"               ,max_weekday string\r\n" + 
				"               ,avg_weekday string\r\n" + 
				"               ,sum_weekday string\r\n" + 
				"               ,std_weekday string\r\n" + 
				"               \r\n" + 
				"               ,max_weekend string\r\n" + 
				"               ,avg_weekend string\r\n" + 
				"               ,sum_weekend string\r\n" + 
				"               ,std_weekend string\r\n" + 
				"               \r\n" + 
				"               ,max_holiday string\r\n" + 
				"               ,avg_holiday string\r\n" + 
				"               ,sum_holiday string\r\n" + 
				"               ,std_holiday string    \r\n" + 
				"               \r\n" + 
				"               ,call_cnt_gre_5 string\r\n" + 
				"               ,call_cnt_equal_1 string\r\n" + 
				"               ,call_cnt_gre_5_ratio string\r\n" + 
				"               ,call_cnt_equal_1_ratio string\r\n" + 
				"               ,call_len_eql_gre_1 string\r\n" + 
				"               ,call_len_les_1 string\r\n" + 
				"\r\n" + 
				"               ,coc_gre_0 string\r\n" + 
				"               ,coc_equal_0 string\r\n" + 
				"               ,coc_gre_0_ratio string\r\n" + 
				"               \r\n" + 
				"               ,cic_gre_0 string\r\n" + 
				"               ,cic_equal_0 string\r\n" + 
				"               ,cic_gre_0_ratio string\r\n" + 
				"               \r\n" + 
				"               ,c1w_gre_0 string\r\n" + 
				"               ,c1w_equal_0 string\r\n" + 
				"               ,c1w_gre_0_ratio string\r\n" + 
				"               \r\n" + 
				"               ,c1m_gre_0 string\r\n" + 
				"               ,c1m_equal_0 string\r\n" + 
				"               ,c1m_gre_0_ratio string\r\n" + 
				"               \r\n" + 
				"               ,c3m_gre_0 string\r\n" + 
				"               ,c3m_equal_0 string\r\n" + 
				"               ,c3m_gre_0_ratio string               \r\n" + 
				"               \r\n" + 
				"               ,ccem_gre_0 string\r\n" + 
				"               ,ccem_equal_0 string\r\n" + 
				"               ,ccem_gre_0_ratio string               \r\n" + 
				"               \r\n" + 
				"               ,ccm_gre_0 string\r\n" + 
				"               ,ccm_equal_0 string\r\n" + 
				"               ,ccm_gre_0_ratio string                 \r\n" + 
				"               \r\n" + 
				"               ,ccn_gre_0 string\r\n" + 
				"               ,ccn_equal_0 string\r\n" + 
				"               ,ccn_gre_0_ratio string \r\n" + 
				"               \r\n" + 
				"               ,ca_gre_0 string\r\n" + 
				"               ,ca_equal_0 string\r\n" + 
				"               ,ca_gre_0_ratio string                \r\n" + 
				"               \r\n" + 
				"               ,cn_gre_0 string\r\n" + 
				"               ,cn_equal_0 string\r\n" + 
				"               ,cn_gre_0_ratio string               \r\n" + 
				"               \r\n" + 
				"               ,ccwd_gre_0 string\r\n" + 
				"               ,ccwd_equal_0 string\r\n" + 
				"               ,ccwd_gre_0_ratio string                \r\n" + 
				"               \r\n" + 
				"               ,ccwe_gre_0 string\r\n" + 
				"               ,ccwe_equal_0 string\r\n" + 
				"               ,ccwe_gre_0_ratio string  \r\n" + 
				"               \r\n" + 
				"               ,count_region string\r\n" + 
				"               ,max_avg_cit string\r\n" + 
				"               ,diffdate string\r\n" + 
				"               ,equipment_app_name string\r\n" + 
				"               ,call_len_then_equal_1_ratio string\r\n" + 
				"               )\r\n" + 
				"   partitioned by(dt string comment '分区日期');\r\n" + 
				"               \r\n" + 
				"               \r\n" + 
				"        SET hive.exec.dynamic.partition=true;\r\n" + 
				"        SET hive.exec.dynamic.partition.mode=nonstrict;\r\n" + 
				"        SET hive.exec.max.dynamic.partitions=100000;\r\n" + 
				"        SET hive.exec.max.dynamic.partitions.pernode=100000;\r\n" + 
				"        insert overwrite table dm_sandbox.dm_regular_customer_biz_wt_sheng partition (dt)\r\n" + 
				"        select apply_risk_id \r\n" + 
				"               ,apply_risk_created_at \r\n" + 
				"               ,overdue_days \r\n" + 
				"               ,max_yuqi_day \r\n" + 
				"               ,min_yuqi_day \r\n" + 
				"               ,mean_yuqi_day \r\n" + 
				"               ,std_yuqi_day \r\n" + 
				"               ,last_latest_created_span \r\n" + 
				"               ,first_last_created_span \r\n" + 
				"               ,mean_first_last_created_span \r\n" + 
				"               ,latest_yuqi_day \r\n" + 
				"               ,farest_yuqi_day \r\n" + 
				"               ,danqi_num \r\n" + 
				"               ,duoqi_num \r\n" + 
				"               ,latest_borrow_span \r\n" + 
				"               ,farest_created_span \r\n" + 
				"               ,mean_created_span \r\n" + 
				"               \r\n" + 
				"               ,baidu_panshi_prea_score \r\n" + 
				"               ,baidu_panshi_duotou_name_score \r\n" + 
				"               ,baidu_panshi_duotou_identity_score \r\n" + 
				"               ,baidu_panshi_duotou_phone_score \r\n" + 
				"               ,baidu_panshi_black_score \r\n" + 
				"               ,baidu_panshi_black_count_level1 \r\n" + 
				"               ,baidu_panshi_black_count_level2 \r\n" + 
				"               ,baidu_panshi_black_count_level3 \r\n" + 
				"               \r\n" + 
				"               ,fbi_score \r\n" + 
				"               ,fbi_desc \r\n" + 
				"               ,xinyan_score \r\n" + 
				"               ,anti_fraud_old \r\n" + 
				"               ,anti_fraud \r\n" + 
				"               ,anti_fraud_version_two_point_zero \r\n" + 
				"               ,xcloud_score \r\n" + 
				"               ,jd_ss_score_payday_sort_score \r\n" + 
				"               ,td_score_final_score \r\n" + 
				"               ,umeng_credit_score_credit_score \r\n" + 
				"               \r\n" + 
				"               ,max_call_in_len \r\n" + 
				"               ,avg_call_in_len\r\n" + 
				"               ,sum_call_in_len \r\n" + 
				"               ,std_call_in_len \r\n" + 
				"               \r\n" + 
				"               ,max_call_cnt \r\n" + 
				"               ,avg_call_cnt \r\n" + 
				"               ,sum_call_cnt \r\n" + 
				"               ,std_call_cnt \r\n" + 
				"               \r\n" + 
				"               ,max_call_len \r\n" + 
				"               ,avg_call_len \r\n" + 
				"               ,sum_call_len \r\n" + 
				"               ,std_call_len \r\n" + 
				"               \r\n" + 
				"               ,max_call_out_cnt \r\n" + 
				"               ,avg_call_out_cnt \r\n" + 
				"               ,sum_call_out_cnt \r\n" + 
				"               ,std_call_out_cnt \r\n" + 
				"               \r\n" + 
				"               ,max_call_out_len \r\n" + 
				"               ,avg_call_out_len \r\n" + 
				"               ,sum_call_out_len \r\n" + 
				"               ,std_call_out_len              \r\n" + 
				"               \r\n" + 
				"               ,max_call_in_cnt \r\n" + 
				"               ,avg_call_in_cnt \r\n" + 
				"               ,sum_call_in_cnt \r\n" + 
				"               ,std_call_in_cnt  \r\n" + 
				"               \r\n" + 
				"               ,max_1w \r\n" + 
				"               ,avg_1w \r\n" + 
				"               ,sum_1w \r\n" + 
				"               ,std_1w \r\n" + 
				"               \r\n" + 
				"               ,count_1m \r\n" + 
				"               ,max_1m \r\n" + 
				"               ,avg_1m \r\n" + 
				"               ,sum_1m \r\n" + 
				"               ,std_1m \r\n" + 
				"               \r\n" + 
				"               ,max_3m \r\n" + 
				"               ,avg_3m \r\n" + 
				"               ,sum_3m \r\n" + 
				"               ,std_3m \r\n" + 
				"               \r\n" + 
				"               ,max_early_morning \r\n" + 
				"               ,avg_early_morning \r\n" + 
				"               ,sum_early_morning \r\n" + 
				"               ,std_early_morning \r\n" + 
				"               \r\n" + 
				"               ,max_morning \r\n" + 
				"               ,avg_morning \r\n" + 
				"               ,sum_morning \r\n" + 
				"               ,std_morning    \r\n" + 
				"               \r\n" + 
				"               ,max_noon \r\n" + 
				"               ,avg_noon \r\n" + 
				"               ,sum_noon \r\n" + 
				"               ,std_noon    \r\n" + 
				"               \r\n" + 
				"               ,max_afternoon \r\n" + 
				"               ,avg_afternoon \r\n" + 
				"               ,sum_afternoon \r\n" + 
				"               ,std_afternoon    \r\n" + 
				"        \r\n" + 
				"               ,max_night \r\n" + 
				"               ,avg_night \r\n" + 
				"               ,sum_night \r\n" + 
				"               ,std_night \r\n" + 
				"               \r\n" + 
				"               ,sum_all_day \r\n" + 
				"               \r\n" + 
				"               ,max_weekday \r\n" + 
				"               ,avg_weekday \r\n" + 
				"               ,sum_weekday \r\n" + 
				"               ,std_weekday \r\n" + 
				"               \r\n" + 
				"               ,max_weekend \r\n" + 
				"               ,avg_weekend \r\n" + 
				"               ,sum_weekend \r\n" + 
				"               ,std_weekend\r\n" + 
				"               \r\n" + 
				"               ,max_holiday \r\n" + 
				"               ,avg_holiday \r\n" + 
				"               ,sum_holiday \r\n" + 
				"               ,std_holiday    \r\n" + 
				"               \r\n" + 
				"               ,call_cnt_gre_5\r\n" + 
				"               ,call_cnt_equal_1 \r\n" + 
				"               ,call_cnt_gre_5_ratio\r\n" + 
				"               ,call_cnt_equal_1_ratio\r\n" + 
				"               ,call_len_eql_gre_1 \r\n" + 
				"               ,call_len_les_1 \r\n" + 
				"\r\n" + 
				"               ,coc_gre_0 \r\n" + 
				"               ,coc_equal_0 \r\n" + 
				"               ,coc_gre_0_ratio \r\n" + 
				"               \r\n" + 
				"               ,cic_gre_0 \r\n" + 
				"               ,cic_equal_0 \r\n" + 
				"               ,cic_gre_0_ratio \r\n" + 
				"               \r\n" + 
				"               ,c1w_gre_0 \r\n" + 
				"               ,c1w_equal_0 \r\n" + 
				"               ,c1w_gre_0_ratio \r\n" + 
				"               \r\n" + 
				"               ,c1m_gre_0 \r\n" + 
				"               ,c1m_equal_0 \r\n" + 
				"               ,c1m_gre_0_ratio \r\n" + 
				"               \r\n" + 
				"               ,c3m_gre_0 \r\n" + 
				"               ,c3m_equal_0 \r\n" + 
				"               ,c3m_gre_0_ratio                \r\n" + 
				"               \r\n" + 
				"               ,ccem_gre_0 \r\n" + 
				"               ,ccem_equal_0 \r\n" + 
				"               ,ccem_gre_0_ratio                \r\n" + 
				"               \r\n" + 
				"               ,ccm_gre_0 \r\n" + 
				"               ,ccm_equal_0 \r\n" + 
				"               ,ccm_gre_0_ratio                  \r\n" + 
				"               \r\n" + 
				"               ,ccn_gre_0 \r\n" + 
				"               ,ccn_equal_0 \r\n" + 
				"               ,ccn_gre_0_ratio  \r\n" + 
				"               \r\n" + 
				"               ,ca_gre_0 \r\n" + 
				"               ,ca_equal_0 \r\n" + 
				"               ,ca_gre_0_ratio                 \r\n" + 
				"               \r\n" + 
				"               ,cn_gre_0 \r\n" + 
				"               ,cn_equal_0 \r\n" + 
				"               ,cn_gre_0_ratio                \r\n" + 
				"               \r\n" + 
				"               ,ccwd_gre_0 \r\n" + 
				"               ,ccwd_equal_0 \r\n" + 
				"               ,ccwd_gre_0_ratio                 \r\n" + 
				"               \r\n" + 
				"               ,ccwe_gre_0 \r\n" + 
				"               ,ccwe_equal_0 \r\n" + 
				"               ,ccwe_gre_0_ratio   \r\n" + 
				"               \r\n" + 
				"               ,count_region \r\n" + 
				"               ,max_avg_cit \r\n" + 
				"               ,diffdate \r\n" + 
				"               ,equipment_app_name \r\n" + 
				"               ,call_len_then_equal_1_ratio \r\n" + 
				"               ,from_unixtime(unix_timestamp(apply_risk_created_at, 'yyyy-MM-dd'), 'yyyyMMdd') as dt\r\n" + 
				"          from dm_temp.dm_regular_customer_biz_wt_1_sheng;";
		
		String[]  temp_hqls = hql1.split(";");
		String raw_sqls [] = new String[temp_hqls.length];
		for(int i=0; i< temp_hqls.length; i++){
			raw_sqls[i] = temp_hqls[i].replaceAll("\n", " ").replace("\r", " ");
		}
		for(String tmp : raw_sqls) {
			System.out.println(tmp.trim());
		}
		
		HiveSqlAnalyzeResult result = HiveTableUtil.getTableTree(raw_sqls);
		System.out.println(result.getHsqls());
		System.out.println(result.getGraph());
	}

}
