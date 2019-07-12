package org.jeecg.sql.blood.vo;

import java.io.Serializable;

public class HIveSqlTableEdgeVo implements Serializable{
	
	private static final long serialVersionUID = -3996771659175066162L;
	
	//头节点
	private String  source;
	//尾节点
	private String target;
	//当前关系的状态【0：未执行，1：正在执行，2：执行成功，3：执行失败】
	private Integer state;

	
	public HIveSqlTableEdgeVo() {}
	
	public HIveSqlTableEdgeVo(String source, String target) {
		this.source = source;
		this.target = target;
	}

	public HIveSqlTableEdgeVo(String source, String target, Integer state) {
		this.source = source;
		this.target = target;
		this.state = state;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getTarget() {
		return target;
	}

	public void setTarget(String target) {
		this.target = target;
	}

	public Integer getState() {
		return state;
	}

	public void setState(Integer state) {
		this.state = state;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((source == null) ? 0 : source.hashCode());
		result = prime * result + ((target == null) ? 0 : target.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
            return true;
        }
		if (obj == null) {
            return false;
        }
		if (getClass() != obj.getClass()) {
            return false;
        }
		HIveSqlTableEdgeVo other = (HIveSqlTableEdgeVo) obj;
		if (source == null) {
			if (other.source != null) {
                return false;
            }
		} else if (!source.equals(other.source)) {
            return false;
        }
		if (target == null) {
			if (other.target != null) {
                return false;
            }
		} else if (!target.equals(other.target)) {
            return false;
        }
		return true;
	}

	@Override
	public String toString() {
		return "HIveSqlTableEdgeVo [source=" + source + ", target=" + target + ", state=" + state + "]";
	}

	
}
