package org.jeecg.sql.blood.vo;

import org.jgrapht.graph.DefaultEdge;

public class HIveSqlTableEdge extends DefaultEdge{
	
	private static final long serialVersionUID = -3996771659175066162L;
	
	//运行过程中的锚点ID
	private String  stageId;
	//与stageId对应，形象直观的步骤ID
	private Integer stageNo;
	//当前关系的状态【0：未执行，1：正在执行，2：执行成功，3：执行失败】
	private Integer stageState;
	//关系描述
	private String description;
	
	public HIveSqlTableEdge() {}
	
	public HIveSqlTableEdge(String stageId) {
		this(stageId, null, null);
	}
	
	public HIveSqlTableEdge(String stageId, Integer stageNo) {
		this(stageId, stageNo, null);
	}

	public HIveSqlTableEdge(String stageId, Integer stageNo, Integer stageState) {
		this(stageId, stageNo, stageState, null);
	}

	public HIveSqlTableEdge(String stageId, Integer stageNo, Integer stageState, String description) {
		this.stageId = stageId;
		this.stageNo = stageNo;
		this.stageState = stageState;
		this.description = description;
	}

	public String getStageId() {
		return stageId;
	}

	public void setStageId(String stageId) {
		this.stageId = stageId;
	}

	public Integer getStageNo() {
		return stageNo;
	}

	public void setStageNo(Integer stageNo) {
		this.stageNo = stageNo;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Integer getStageState() {
		return stageState;
	}

	public void setStageState(Integer stageState) {
		this.stageState = stageState;
	}
	
	@Override
	public String toString() {
		return  String.valueOf(stageNo) + ":" + String.valueOf(stageState);
	}
}
