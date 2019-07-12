package org.jeecg.express;

import com.ql.util.express.DefaultContext;

import java.util.HashMap;
import java.util.Map;

public class ExpressRunnerTest {
	
	
	
	public static void main(String[] args) throws Exception {
		Map<String, Object> parms = new HashMap<>();
		parms.put("flag", 2);
		
		Map<String, Object> rowMap = new HashMap<>();
		rowMap.put("over_days", 8);
		rowMap.put("c2", "test-222");
		rowMap.put("c3", "test-333");
		
		DefaultContext<String, Object> context = new DefaultContext<String, Object>();
		context.put("row", rowMap);
		context.put("params", parms);
		
		String express = ""
				+ ""
				+ "		if(row.over_days > params.flag){return row.over_days;} else{return params.flag;}"
				+ " "
				+ "";
		
        Object r = ExpressFactory.transform(new ExpressModel(express, rowMap, parms));;
		System.out.println(r);
	}
	
}
