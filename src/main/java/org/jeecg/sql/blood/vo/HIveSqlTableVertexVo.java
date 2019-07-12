package org.jeecg.sql.blood.vo;

import java.io.Serializable;

public class HIveSqlTableVertexVo implements Serializable{

	private static final long serialVersionUID = -2761708446045917571L;
	
	private String name;
	private Integer state;
	private String label;
	
	public HIveSqlTableVertexVo(String name) {
		this.name = name;
	}
	
	public HIveSqlTableVertexVo(String name, String label) {
		this.name = name;
		this.label = label;
	}

	public HIveSqlTableVertexVo(String name, Integer state, String label) {
		this.name = name;
		this.state = state;
		this.label = label;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public Integer getState() {
		return state;
	}

	public void setState(Integer state) {
		this.state = state;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
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
		HIveSqlTableVertexVo other = (HIveSqlTableVertexVo) obj;
		if (name == null) {
			if (other.name != null) {
                return false;
            }
		} else if (!name.equals(other.name)) {
            return false;
        }
		return true;
	}

	@Override
	public String toString() {
		return name;
	}
	
}
