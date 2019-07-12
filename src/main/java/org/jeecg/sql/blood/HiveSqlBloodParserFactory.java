package org.jeecg.sql.blood;

import org.jeecg.sql.blood.model.SQLResult;
import org.jeecg.sql.blood.parse.HiveSqlBloodFigureParser;

import java.util.List;

public class HiveSqlBloodParserFactory {
	
	/**
	 * 解析字段血缘
	 * @param hsql	sqls
	 * @return	血缘关系
	 * @throws Exception
	 */
	public static List<SQLResult> parser(String[] hsql) throws Exception{
		return parser(hsql, null);
	}
	
	/**
	 * 解析字段血缘
	 * @param hsql	sqls
	 * @return	血缘关系
	 * @throws Exception
	 */
	public static List<SQLResult> parser(String[] hsql, HiveJdbcTemplate template) throws Exception{
		StringBuffer buffer = new StringBuffer();
		if(hsql != null) {
			for(String item : hsql) {
				item = item.trim();
				buffer.append(item);
				if(!item.endsWith(";")) {
					buffer.append(";");
				}
			}
		}
		return parser(buffer.toString(), template);
	}
	
	
	/**
	 * 解析字段血缘
	 * @param hsql	sqls
	 * @return	血缘关系
	 * @throws Exception
	 */
	public static List<SQLResult> parser(List<String> hsql) throws Exception{
		return parser(hsql, null);
	}
	
	
	/**
	 * 解析字段血缘
	 * @param hsql	sqls
	 * @return	血缘关系
	 * @throws Exception
	 */
	public static List<SQLResult> parser(List<String> hsql, HiveJdbcTemplate template) throws Exception{
		StringBuffer buffer = new StringBuffer();
		if(hsql != null) {
			for(String item : hsql) {
				item = item.trim();
				buffer.append(item);
				if(!item.endsWith(";")) {
					buffer.append(";");
				}
			}
		}
		return parser(buffer.toString(), template);
	}
	
	
	/**
	 * 解析字段血缘
	 * @param hsql	sqls
	 * @return	血缘关系
	 * @throws Exception
	 */
	public static List<SQLResult> parser(String hsql) throws Exception{
		return parser(hsql, null);
	}
	
	/**
	 * 解析字段血缘
	 * @param hsql	sqls
	 * @return	血缘关系
	 * @throws Exception
	 */
	public static List<SQLResult> parser(String hsql, HiveJdbcTemplate template) throws Exception{
		return new HiveSqlBloodFigureParser().parse(hsql, template);
	}
	
	
	
}
