package org.jeecg.sql.blood.vo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class HIveSqlTableGraphVo implements Serializable{
	
	private static final long serialVersionUID = -205107165418459541L;
	
	private List<HIveSqlTableVertexVo> nodes = new ArrayList<>();
	private List<HIveSqlTableEdgeVo>   edges = new ArrayList<>();
	
	public HIveSqlTableGraphVo(){}
	
	public HIveSqlTableGraphVo(List<HIveSqlTableVertexVo> nodes, List<HIveSqlTableEdgeVo> edges) {
		this.nodes = nodes;
		this.edges = edges;
	}

	public List<HIveSqlTableVertexVo> getNodes() {
		return nodes;
	}

	public void setNodes(List<HIveSqlTableVertexVo> nodes) {
		this.nodes = nodes;
	}

	public List<HIveSqlTableEdgeVo> getEdges() {
		return edges;
	}

	public void setEdges(List<HIveSqlTableEdgeVo> edges) {
		this.edges = edges;
	}

	@Override
	public String toString() {
		return "HIveSqlTableGraphVo [nodes=" + nodes + ", edges=" + edges + "]";
	}
	
}
