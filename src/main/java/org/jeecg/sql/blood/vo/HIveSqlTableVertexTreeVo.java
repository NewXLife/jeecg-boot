package org.jeecg.sql.blood.vo;

import java.util.HashSet;
import java.util.Set;

public class HIveSqlTableVertexTreeVo extends HIveSqlTableVertexVo{

	private static final long serialVersionUID = 2510598534973174680L;
	
	private Set<HIveSqlTableVertexTreeVo> children = new HashSet<>();

	public HIveSqlTableVertexTreeVo(String name) {
		super(name);
	}

	public Set<HIveSqlTableVertexTreeVo> getChildren() {
		return children;
	}

	public void setChildren(Set<HIveSqlTableVertexTreeVo> children) {
		this.children = children;
	}
	
}
