package org.jeecg.sql.blood.vo;

import java.io.Serializable;

public class HiveUdfParam implements Serializable{
	
	private static final long serialVersionUID = 8051522735367576355L;
	
	private String executeId;
	private String stageId;
	private String state;
	
	public HiveUdfParam(){}
	
	public HiveUdfParam(String executeId, String stageId, String state) {
		this.executeId = executeId;
		this.stageId = stageId;
		this.state = state;
	}

	public String getExecuteId() {
		return executeId;
	}

	public void setExecuteId(String executeId) {
		this.executeId = executeId;
	}

	public String getStageId() {
		return stageId;
	}

	public void setStageId(String stageId) {
		this.stageId = stageId;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	@Override
	public String toString() {
		return "HiveUdfParam [executeId=" + executeId + ", stageId=" + stageId + ", state=" + state + "]";
	}
	
	/**判断是不是参数**/
	public boolean isParam(){
		if(this.stageId != null){
			return this.stageId.startsWith("_");
		}
		return false;
	}
	
}
