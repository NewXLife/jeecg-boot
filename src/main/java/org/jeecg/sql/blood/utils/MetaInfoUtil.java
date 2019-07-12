package org.jeecg.sql.blood.utils;

import org.apache.commons.lang.StringEscapeUtils;
import org.jeecg.sql.blood.model.Field;
import org.jeecg.sql.blood.model.MetaInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MetaInfoUtil
{
	
	/**
	 * 获取查询元数据信息的sql
	 * @param database
	 * @param table
	 * @return
	 */
	public static String getMetaSql(String database, String table) {
		//拼装sql
		StringBuffer sbf = new StringBuffer();
		sbf.append("desc formatted").append(" ");
		if(database != null && !database.trim().isEmpty()) {
			sbf.append(database.trim()).append(".");
		}
		sbf.append(table.trim());
		return sbf.toString();
	}
	
	/**
	 * 获取元数据信息
	 * @param res
	 * @return
	 */
	public static MetaInfo getMetaInfo(List<Map<String, Object>> res) {
		//查询元数据信息
		boolean start_fields 	 = true;
		boolean start_partitions = false;
		boolean start_detailed   = false;
		//临时变量
		String tableComment = null;
		List<Field> fields = new ArrayList<>();
		List<Field> partitions = new ArrayList<>();
		
		for(Map<String, Object> item : res) {
			String col_name  = item.get("col_name")  != null ? item.get("col_name").toString().trim()  : null;
			String data_type = item.get("data_type") != null ? item.get("data_type").toString().trim() : null;
			String comment   = item.get("comment") 	 != null ? item.get("comment").toString().trim()   : null;
			//过滤错误的注释信息
			if(comment != null && comment.toLowerCase().contains("deserializer")) {
				comment = null;
			}
			//分区信息开始
			if(col_name.contains("#") && col_name.contains("Partition") && col_name.contains("Information")) {
				start_partitions = true;
			}
			//详细信息开始
			if(col_name.contains("#") && col_name.contains("Detailed") && col_name.contains("Information")) {
				start_fields = false;
				start_partitions = false;
				start_detailed = true;
			}
			//获取信息
			if(start_fields && !col_name.contains("#")) {
				if(col_name!=null && !col_name.isEmpty() && data_type != null && !data_type.isEmpty()) {
					fields.add(new Field(col_name, data_type, comment));
				}
			}
			if(start_partitions && !col_name.contains("#")) {
				if(col_name!=null && !col_name.isEmpty() && data_type != null && !data_type.isEmpty()) {
					partitions.add(new Field(col_name, data_type, comment));
				}
			}
			if(start_detailed) {
				if("comment".equalsIgnoreCase(data_type)) {
					tableComment = StringEscapeUtils.unescapeJava(comment);
				}
			}
		}
		return new MetaInfo(tableComment, fields, partitions);
	}
	
}
