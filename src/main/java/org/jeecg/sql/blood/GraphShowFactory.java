package org.jeecg.sql.blood;

import com.mxgraph.layout.hierarchical.mxHierarchicalLayout;
import com.mxgraph.layout.mxParallelEdgeLayout;
import com.mxgraph.swing.mxGraphComponent;
import org.jeecg.sql.blood.model.TableEdge;
import org.jeecg.sql.blood.model.TableVertex;
import org.jgrapht.Graph;
import org.jgrapht.ext.JGraphXAdapter;
import org.jgrapht.graph.AsSubgraph;
import org.jgrapht.graph.DefaultDirectedGraph;

import javax.swing.*;
import java.util.Map;
import java.util.Set;

public class GraphShowFactory {
	
	/**
	 * 	展示血缘图
	 * @param graph
	 */
	public static void show(Graph<TableVertex, TableEdge> graph) {
		JGraphXAdapter<TableVertex, TableEdge> graphx= new JGraphXAdapter<>(graph);
		mxGraphComponent graphComponent = new mxGraphComponent(graphx);
        JFrame frame = new JFrame();
        frame.getContentPane().add(graphComponent);
        new mxHierarchicalLayout(graphx).execute(graphx.getDefaultParent());
        new mxParallelEdgeLayout(graphx).execute(graphx.getDefaultParent());
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.setSize(800, 600);
		frame.setVisible(true);
	}
	
	/**
	 * 	展示血缘图
	 * @param graphs
	 */
	public static void show(Map<TableVertex, AsSubgraph<TableVertex, TableEdge>> graphs) {
		Graph<TableVertex, TableEdge> graph = new DefaultDirectedGraph<>(TableEdge.class);
		for(TableVertex vertex : graphs.keySet()) {
			AsSubgraph<TableVertex, TableEdge> subgraph = graphs.get(vertex);
			Set<TableVertex> vertexs = subgraph.vertexSet();
			for(TableVertex vertex2 : vertexs) {
				graph.addVertex(vertex2);
			}
			Set<TableEdge> edges = subgraph.edgeSet();
			for(TableEdge edge2 : edges) {
				graph.addEdge(subgraph.getEdgeSource(edge2), subgraph.getEdgeTarget(edge2), edge2);
			}
		}
		show(graph);
	}
	
}
