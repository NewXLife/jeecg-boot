package org.jeecg.sql.blood.utils;

import org.jeecg.sql.blood.HiveJdbcTemplate;
import org.jeecg.sql.blood.model.ColumnNode;
import org.jeecg.sql.blood.model.Field;
import org.jeecg.sql.blood.model.MetaInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetaCache {
	private static MetaCache instance = new MetaCache();
	
	private static Map<String, List<ColumnNode>> cMap = new HashMap<String, List<ColumnNode>>();
	private static Map<String, Long> tableMap = new HashMap<String, Long>();
	private static Map<String, Long> columnMap = new HashMap<String, Long>();

	private HiveJdbcTemplate template;
	
	private MetaCache(){}
	
	public static MetaCache getInstance(HiveJdbcTemplate template){
		if(template!=null && template!=instance.getTemplate()) {
			instance.setTemplate(template);
		}
		return instance;
	}
	
	public void init(String table){
		if(this.getTemplate() == null) {
			return;
		}
		try {
			String[] pdt = ParseUtil.parseDBTable(table);
			MetaInfo metaInfo = this.getTemplate().getMetaInfo(pdt[0], pdt[1]);
			List<ColumnNode> list = new ArrayList<>();
			for(Field field : metaInfo.getFields()) {
				ColumnNode node = new ColumnNode();
				node.setDb(pdt[0]);
				node.setTable(pdt[1]);
				node.setColumn(field.getName());
				list.add(node);
			}
			if (Check.notEmpty(list)) {
				cMap.put(table.toLowerCase(), list);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public void release(){
		cMap.clear();
		tableMap.clear();
		columnMap.clear();
	}
	
	public List<String> getColumnByDBAndTable(String table){
		List<ColumnNode> list = cMap.get(table.toLowerCase());
		List<String> list2 = new ArrayList<String>();
		if (Check.notEmpty(list)) {
			for (ColumnNode columnNode : list) {
				list2.add(columnNode.getColumn());
			}
		}
		return list2;
	}
	
	public Map<String, List<ColumnNode>> getcMap() {
		return cMap;
	}

	public Map<String, Long> getTableMap() {
		return tableMap;
	}

	public Map<String, Long> getColumnMap() {
		return columnMap;
	}

	public HiveJdbcTemplate getTemplate() {
		return template;
	}

	public void setTemplate(HiveJdbcTemplate template) {
		this.template = template;
	}
}
