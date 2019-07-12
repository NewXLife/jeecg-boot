package org.jeecg.sql.blood.model;

import org.jgrapht.graph.DefaultEdge;

public class TableEdge extends DefaultEdge{
	
	private static final long serialVersionUID = -3996771659175066162L;

	private String relation;
	
	public TableEdge() {}
	
	public TableEdge(String relation) {
		this.relation = relation;
	}

	public String getRelation() {
		return relation;
	}

	public void setRelation(String relation) {
		this.relation = relation;
	}

	@Override
	public String toString() {
		return relation;
	}

}
