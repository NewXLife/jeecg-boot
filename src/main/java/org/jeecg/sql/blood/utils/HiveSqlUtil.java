package org.jeecg.sql.blood.utils;

import org.jeecg.sql.blood.vo.HiveUdfParam;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class HiveSqlUtil {
	
	private static final String regex = "'(.*?)'";
	
	/**
	 * 获取UDF的参数信息
	 * @param hql	sql
	 * @return	HiveUdfParam
	 */
	private static HiveUdfParam getHiveUdfParam(String hql){
		Pattern p = Pattern.compile(regex);  
		Matcher m = p.matcher(hql);  
		HiveUdfParam param = new HiveUdfParam();
		int i = 0;
		while(m.find()){  
			i ++ ;
			switch (i) {
			case 1:
				String executeId = "null".equals(m.group(1)) ? null : m.group(1);
				param.setExecuteId(executeId);
				break;
			case 2:
				String stageId = "null".equals(m.group(1)) ? null : m.group(1);
				param.setStageId(stageId);
				break;
			case 3:
				String stageState = "null".equals(m.group(1)) ? null : m.group(1);
				param.setState(stageState);
				break;
			default:
				break;
			}
	    }  
		if(i == 3){
			return param;
		}
		else{
			return null;
		}
	}
	
	/**
	 *  获取需要重新计算的sql
	 * @param hqls	sql脚本
	 * @param stages	执行状态
	 * @return	sql
	 */
	public static List<String> getReTrySqls(String hqls[], Set<String> stages){
		return getReTrySqls(hqls, stages, null);
	}
	
	/**
	 * 获取需要重新计算的sql, 并替换execute_id参数信息
	 * @param hqls	sql脚本
	 * @param stages	执行状态
	 * @param execute_id	当前的执行ID
	 * @return	sql
	 */
	public static List<String> getReTrySqls(String hqls[], Set<String> stages, String execute_id){
		List<String> resqls = new ArrayList<>();
		boolean flag = true;
		for(String hql : hqls){
			HiveUdfParam param = getHiveUdfParam(hql);
			if(param != null && !param.isParam()){
				String this_stageId = param.getStageId();
				if(stages.contains(this_stageId)){
					flag = false;
				}
				else{
					flag = true;
				}
				//替换ID
				if(execute_id != null){
					hql = hql.replace(param.getExecuteId(), execute_id);
				}
			}
			if(flag){
				resqls.add(hql);
			}
		}
		return resqls;
	}
	
	public static void main(String[] args) {
//		String s = "select ip_sp_udf_tracking('${hiveconf:execute_id}', '_edfb41fd68e248abb19e2e010def901f', '10');";
//		s = "select ip_sp_udf_tracking('${hiveconf:execute_id}', 'null', '0');";
		
		String 	s = "select ip_sp_udf_tracking('${hiveconf:execute_id}', 'null', '0');\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', '_4df871d2cb87424d83405e84dbadd4ee', '10');\n"
				+ "use test;\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', '_4df871d2cb87424d83405e84dbadd4ee', '11');\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', '85f2481a1db1457b918d83681d0a0647', '10');\n"
				+ "drop table if exists temp.b1;\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', '85f2481a1db1457b918d83681d0a0647', '11');\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', 'b5a1ed33c3a04064971b847f83edbf21', '10');\n"
				+ "drop table if exists temp.b2;\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', 'b5a1ed33c3a04064971b847f83edbf21', '11');\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', '566743966c0740728f310f66bee062dc', '10');\n"
				+ "drop table if exists temp.c1;\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', '566743966c0740728f310f66bee062dc', '11');\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', '06d4199eb1b84b43b3669879a65ffea7', '10');\n"
				+ "drop table if exists temp.c2;\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', '06d4199eb1b84b43b3669879a65ffea7', '11');\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', '94b7d5f99434494980308744e5f53fde', '10');\n"
				+ "drop table if exists temp.d1;\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', '94b7d5f99434494980308744e5f53fde', '11');\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', '276270120663476fbd8abce5bc85f513', '10');\n"
				+ "create table temp.b1(id string, name string) row format delimited fields terminated by ',';\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', '276270120663476fbd8abce5bc85f513', '11');\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', '1ba28dcab4a04ea9bbf170b79bebeb4d', '10');\n"
				+ "create table temp.b2(id string, age int) row format delimited fields terminated by ',';\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', '1ba28dcab4a04ea9bbf170b79bebeb4d', '11');\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', 'f238ab6f1a2c4011a0dbf19b9918fd71', '10');\n"
				+ "create table temp.c1(id string, name string) row format delimited fields terminated by ',';\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', 'f238ab6f1a2c4011a0dbf19b9918fd71', '11');\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', '53965fff19994046991eeda1bd4a8535', '10');\n"
				+ "create table temp.c2(id string, age int) row format delimited fields terminated by ',';\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', '53965fff19994046991eeda1bd4a8535', '11');\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', 'd9f3deb4f94c44b9b4a7f70d2377190b', '10');\n"
				+ "create table temp.d1(id string, name string, age int) row format delimited fields terminated by ',';\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', 'd9f3deb4f94c44b9b4a7f70d2377190b', '11');\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', '21f47028de614f638a62bcb30833d07f', '10');\n"
				+ "from temp.a1 insert into table temp.b1 select id, name insert into table temp.b2 select id, age;\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', '21f47028de614f638a62bcb30833d07f', '11');\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', '76f029b978b345e3baa3613799999d68', '10');\n"
				+ "insert overwrite table temp.c1 select id, name from temp.b1;\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', '76f029b978b345e3baa3613799999d68', '11');\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', '882e5e2c90324bc681e73b44abc58aa4', '10');\n"
				+ "insert overwrite table temp.c2 select id, age from temp.b2;\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', '882e5e2c90324bc681e73b44abc58aa4', '11');\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', 'b0578c5861e747aaad05c70e9be19820', '10');\n"
				+ "insert overwrite table temp.d1 select t1.id, t1.name, t2.age from temp.c1 as t1 left join temp.c2 as t2 on t1.id = t2.id;\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', 'b0578c5861e747aaad05c70e9be19820', '11');\n"
				+ "select ip_sp_udf_tracking('${hiveconf:execute_id}', 'null', '1');";
		String hqls[] = s.split("\n");
		
		Set<String> stages = new HashSet<>();
		stages.add("4df871d2cb87424d83405e84dbadd4ee");
		stages.add("85f2481a1db1457b918d83681d0a0647");
		stages.add("b5a1ed33c3a04064971b847f83edbf21");
		
		System.out.println(getReTrySqls(hqls, stages, "992e5e2890324bc681e73b44abc58aa4"));
	}
	
}
