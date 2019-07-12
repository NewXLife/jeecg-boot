package org.jeecg.sql.blood.utils;

import org.jeecg.sql.blood.model.TableEdge;
import org.jeecg.sql.blood.model.TableVertex;
import org.jeecg.sql.blood.vo.HIveSqlTableVertexTreeVo;
import org.jgrapht.Graph;

import java.util.HashSet;
import java.util.Set;

public class GraphToTreeUtil {
	
	public static HIveSqlTableVertexTreeVo convert(TableVertex vertex, Graph<TableVertex, TableEdge> graph) {
		HIveSqlTableVertexTreeVo vo = new HIveSqlTableVertexTreeVo(vertex.getName());
		//获取与顶点有关的所有关系
		Set<TableEdge> edges = new HashSet<>();
		edges.addAll(graph.edgesOf(vertex));
		//子 节点
		Set<HIveSqlTableVertexTreeVo> children = new HashSet<>();
		for(TableEdge edge : edges) {
			//获取相关节点
			TableVertex sourceVertex = graph.getEdgeSource(edge);
			TableVertex targetVertex = graph.getEdgeTarget(edge);
			//删除关系节点
			graph.removeEdge(edge);
			//添加并递回
			if(!sourceVertex.getName().equals(vertex.getName())) {
				children.add(convert(sourceVertex, graph));
			}
			if(!targetVertex.getName().equals(vertex.getName())) {
				children.add(convert(targetVertex, graph));
			}
		}
		//添加子节点
		vo.setChildren(children);
		return vo;
	}
	
}
