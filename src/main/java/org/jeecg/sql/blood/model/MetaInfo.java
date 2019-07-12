package org.jeecg.sql.blood.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class MetaInfo implements Serializable{

	private static final long serialVersionUID = -8873192073858120327L;
	/**数据库描述信息**/
	private String comment;
	/**注意当前属性包含所有字段，即包含分区字段**/
	private List<Field> fields = new ArrayList<>();
	/**分区字段**/
	private List<Field> partitions = new ArrayList<>();
	
	public MetaInfo() {}
	
	public MetaInfo(List<Field> fields) {
		this.fields = fields;
	}
	
	public MetaInfo(String comment, List<Field> fields) {
		this.comment = comment;
		this.fields = fields;
	}

	public MetaInfo(List<Field> fields, List<Field> partitions) {
		this.fields = fields;
		this.partitions = partitions;
	}
	
	public MetaInfo(String comment, List<Field> fields, List<Field> partitions) {
		this.comment = comment;
		this.fields = fields;
		this.partitions = partitions;
	}
	
	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public List<Field> getFields() {
		return fields;
	}

	public void setFields(List<Field> fields) {
		this.fields = fields;
	}

	public List<Field> getPartitions() {
		return partitions;
	}

	public void setPartitions(List<Field> partitions) {
		this.partitions = partitions;
	}
	
	/**
	 *	返回除分区字段的所有字段
	 *  @return
	 */
	public List<Field> excludePartitionsFields(){
		if(this.partitions.isEmpty()) {
			return this.fields;
		}
		List<Field> temps = new ArrayList<>();
		for(Field field : this.fields) {
			if(!this.partitions.contains(field)) {
				temps.add(field);
			}
		}
		return temps;
	}

	@Override
	public String toString() {
		return "MetaInfo [comment=" + comment + ", fields=" + fields + ", partitions=" + partitions + "]";
	}
}
