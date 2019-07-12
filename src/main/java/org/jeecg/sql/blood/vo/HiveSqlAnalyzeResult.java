package org.jeecg.sql.blood.vo;

import org.jgrapht.graph.AbstractBaseGraph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class HiveSqlAnalyzeResult implements Serializable{
	
	private static final long serialVersionUID = -8393467251086870668L;
	
	private List<String> hsqls = new ArrayList<>();
	private AbstractBaseGraph<HIveSqlTableVertex, HIveSqlTableEdge> graph;
	
	public HiveSqlAnalyzeResult(List<String> hsqls, AbstractBaseGraph<HIveSqlTableVertex, HIveSqlTableEdge> graph) {
		this.hsqls = hsqls;
		this.graph = graph;
	}

	public List<String> getHsqls() {
		return hsqls;
	}

	public AbstractBaseGraph<HIveSqlTableVertex, HIveSqlTableEdge> getGraph() {
		return graph;
	}

	@Override
	public String toString() {
		return "HiveSqlAnalyzeResult [hsqls=" + hsqls + ", graph=" + graph + "]";
	}
	
}
