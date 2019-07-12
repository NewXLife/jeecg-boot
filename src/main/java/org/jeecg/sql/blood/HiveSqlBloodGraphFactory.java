package org.jeecg.sql.blood;

import org.jeecg.sql.blood.model.*;
import org.jgrapht.graph.AsSubgraph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.traverse.BreadthFirstIterator;

import java.io.Serializable;
import java.util.*;


public class HiveSqlBloodGraphFactory implements Serializable{

    private static final long serialVersionUID = -5653286080613813645L;

    /**是否开启字段推断**/
    private boolean speculate = false;
    /**字段血缘解析结果**/
    private List<SQLResult> results = new ArrayList<>();

    private HiveSqlBloodGraphFactory(List<SQLResult> results) {
        this.results = results;
    }

    private HiveSqlBloodGraphFactory(List<SQLResult> results, boolean speculate) {
        this.results = results;
        this.speculate = speculate;
    }

    /**
     * 创建对象
     * @param results
     * @return
     */
    public static HiveSqlBloodGraphFactory create(List<SQLResult> results) {
        return new HiveSqlBloodGraphFactory(results);
    }

    /**
     * 创建对象
     * @param results
     * @param speculate
     * @return
     */
    public static HiveSqlBloodGraphFactory create(List<SQLResult> results, boolean speculate) {
        return new HiveSqlBloodGraphFactory(results, speculate);
    }


    /**
     * 	 获取表血缘
     *  @return 表血缘
     */
    public DefaultDirectedGraph<TableVertex, TableEdge> getTableBloodGraph() {
        return this.getTableBloodGraph(true);
    }

    /**
     * 	获取表血缘
     *  @param forward	正向关系与逆向关系
     *  @return	表血缘
     */
    public DefaultDirectedGraph<TableVertex, TableEdge> getTableBloodGraph(boolean forward) {
        DefaultDirectedGraph<TableVertex, TableEdge> graph = new DefaultDirectedGraph<>(TableEdge.class);
        for(SQLResult item : results){
            //表节点信息
            for(String table : item.getInputTables()) {
                graph.addVertex(new TableVertex(TableLable.LABLE_TABLE, table));
            }
            for(String table : item.getOutputTables()) {
                graph.addVertex(new TableVertex(TableLable.LABLE_TABLE, table));
            }
            //表依赖信息
            for(String start : item.getInputTables()) {
                for(String end : item.getOutputTables()) {
                    if(forward) {
                        graph.addEdge(
                                new TableVertex(TableLable.LABLE_TABLE, start),
                                new TableVertex(TableLable.LABLE_TABLE, end),
                                new TableEdge(TableRelation.REL_TABLE_TO_TABLE));
                    }
                    else {
                        graph.addEdge(
                                new TableVertex(TableLable.LABLE_TABLE, end),
                                new TableVertex(TableLable.LABLE_TABLE, start),
                                new TableEdge(TableRelation.REL_TABLE_TO_TABLE));
                    }
                }
            }
        }
        return graph;
    }

    /**
     *  获取字段血缘关系（不包含表血缘）
     *  @return	字段血缘图
     */
    public DefaultDirectedGraph<TableVertex, TableEdge> getFieldBloodGraph() {
        return this.getFieldBloodGraph(true);
    }

    /**
     * 获取字段血缘关系（不包含表血缘）
     * @param forward	正向关系与逆向关系
     * @return	字段血缘图
     */
    public DefaultDirectedGraph<TableVertex, TableEdge> getFieldBloodGraph(boolean forward) {
        DefaultDirectedGraph<TableVertex, TableEdge> graph = new DefaultDirectedGraph<>(TableEdge.class);
        //添加字段节点
        for(SQLResult item : results){
            for(ColLine colLine :item.getColLineList()){
                //字段-源信息表
                for(String from : colLine.getFromNameSet(true)) {
                    graph.addVertex(new TableVertex(TableLable.LABLE_FIELD, from));
                }
                //字段-目标表
                graph.addVertex(new TableVertex(TableLable.LABLE_FIELD, colLine.getToTableFieldParse(speculate)));
            }
        }
        //添加节点关系
        for(SQLResult item : results){
            for(ColLine colLine :item.getColLineList()){
                for(String from : colLine.getFromNameSet(true)) {
                    if(forward) {
                        graph.addEdge(
                                new TableVertex(TableLable.LABLE_FIELD, from),
                                new TableVertex(TableLable.LABLE_FIELD, colLine.getToTableFieldParse(speculate)),
                                new TableEdge(TableRelation.REL_FIELD_TO_FIELD));
                    }
                    else {
                        graph.addEdge(
                                new TableVertex(TableLable.LABLE_FIELD, colLine.getToTableFieldParse(speculate)),
                                new TableVertex(TableLable.LABLE_FIELD, from),
                                new TableEdge(TableRelation.REL_FIELD_TO_FIELD));
                    }
                }
            }
        }
        return graph;
    }

    /**
     *	 获取表-属性图
     *  @return
     */
    public DefaultDirectedGraph<TableVertex, TableEdge> getTableFieldBloodGraph() {
        return this.getTableFieldBloodGraph(true);
    }

    /**
     *	 获取表-属性图
     *  @param forward
     *  @return
     */
    public DefaultDirectedGraph<TableVertex, TableEdge> getTableFieldBloodGraph(boolean forward) {
        DefaultDirectedGraph<TableVertex, TableEdge> graph = new DefaultDirectedGraph<>(TableEdge.class);
        //节点信息
        for(SQLResult item : results){
            //表节点信息
            for(String table : item.getInputTables()) {
                graph.addVertex(new TableVertex(TableLable.LABLE_TABLE, table));
            }
            for(String table : item.getOutputTables()) {
                graph.addVertex(new TableVertex(TableLable.LABLE_TABLE, table));
            }
            for(ColLine colLine :item.getColLineList()){
                //字段-源信息表
                for(String from : colLine.getFromNameSet(true)) {
                    graph.addVertex(new TableVertex(TableLable.LABLE_FIELD, from));
                }
                //字段-目标表
                graph.addVertex(new TableVertex(TableLable.LABLE_FIELD, colLine.getToTableFieldParse(speculate)));
            }
        }
        //添加节点关系
        for(SQLResult item : results){
            for(ColLine colLine :item.getColLineList()){
                //字段-属性-源信息表
                for(String from : colLine.getFromNameSet(true)) {
                    if(forward) {
                        graph.addEdge(
                                new TableVertex(TableLable.LABLE_TABLE, from.substring(0, from.lastIndexOf("."))),
                                new TableVertex(TableLable.LABLE_FIELD, from),
                                new TableEdge(TableRelation.REL_TABLE_TO_FIELD));
                    }
                    else {
                        graph.addEdge(
                                new TableVertex(TableLable.LABLE_FIELD, from),
                                new TableVertex(TableLable.LABLE_TABLE, from.substring(0, from.lastIndexOf("."))),
                                new TableEdge(TableRelation.REL_TABLE_TO_FIELD));
                    }
                }
                //字段-属性-目标表
                if(forward) {
                    graph.addEdge(
                            new TableVertex(TableLable.LABLE_TABLE, colLine.getToTable()),
                            new TableVertex(TableLable.LABLE_FIELD, colLine.getToTableFieldParse(speculate)),
                            new TableEdge(TableRelation.REL_TABLE_TO_FIELD));
                }
                else {
                    graph.addEdge(
                            new TableVertex(TableLable.LABLE_FIELD, colLine.getToTableFieldParse(speculate)),
                            new TableVertex(TableLable.LABLE_TABLE, colLine.getToTable()),
                            new TableEdge(TableRelation.REL_TABLE_TO_FIELD));
                }
            }
        }
        return graph;
    }

    /**
     * 获取全血缘
     * @return 全血缘
     */
    public DefaultDirectedGraph<TableVertex, TableEdge> getCompleteBloodGraph() {
        return this.getCompleteBloodGraph(true, true, true);
    }

    /**
     * 获取全血缘
     * @param tableForward	表依赖 方向
     * @param tableFieldForward		表-列 属性 方向
     * @param fieldForward	字段关联 方向
     * @return 全血缘
     */
    public DefaultDirectedGraph<TableVertex, TableEdge> getCompleteBloodGraph(boolean tableForward, boolean tableFieldForward, boolean fieldForward) {
        DefaultDirectedGraph<TableVertex, TableEdge> tableGraph = this.getTableBloodGraph(tableForward);
        DefaultDirectedGraph<TableVertex, TableEdge> tableFieldGraph = this.getTableFieldBloodGraph(tableFieldForward);
        DefaultDirectedGraph<TableVertex, TableEdge> fieldGraph = this.getFieldBloodGraph(fieldForward);
        List<DefaultDirectedGraph<TableVertex, TableEdge>> graphs = new ArrayList<>();
        graphs.add(tableGraph);graphs.add(tableFieldGraph); graphs.add(fieldGraph);
        DefaultDirectedGraph<TableVertex, TableEdge> completeGraph = new DefaultDirectedGraph<>(TableEdge.class);
        for(DefaultDirectedGraph<TableVertex, TableEdge> graph : graphs) {
            Set<TableVertex> vertexs = graph.vertexSet();
            for(TableVertex vertex2 : vertexs) {
                completeGraph.addVertex(vertex2);
            }
            Set<TableEdge> edges = graph.edgeSet();
            for(TableEdge edge2 : edges) {
                completeGraph.addEdge(graph.getEdgeSource(edge2), graph.getEdgeTarget(edge2), edge2);
            }
        }
        return completeGraph;
    }

    /**
     * 根据字段，追溯其字段血缘
     * @param verte	字段节点
     * @return	字段血缘
     */
    public AsSubgraph<TableVertex, TableEdge> getSingleFieldBloodChain(TableVertex vertex) {
        return this.getSingleFieldBloodChain(vertex, true);
    }

    /**
     * 根据字段，追溯其字段血缘
     * @param verte	字段节点
     * @param forward
     * @return	字段血缘
     */
    public AsSubgraph<TableVertex, TableEdge> getSingleFieldBloodChain(TableVertex vertex, boolean forward) {
        if(TableLable.LABLE_FIELD.equalsIgnoreCase(vertex.getLable())) {
            DefaultDirectedGraph<TableVertex, TableEdge> graph = this.getFieldBloodGraph(forward);
            if(graph.containsVertex(vertex)) {
                BreadthFirstIterator<TableVertex, TableEdge> breadth = new BreadthFirstIterator<>(graph, vertex);
                Set<TableVertex> subNode = new HashSet<>();
                while (breadth.hasNext()) {
                    subNode.add(breadth.next());
                }
                return new AsSubgraph<>(graph, subNode);
            }
        }
        return null;
    }

    /**
     * 根据表节点，追溯其字段血缘
     * @param verte	字段节点
     * @return	字段血缘
     */
    public Map<TableVertex, AsSubgraph<TableVertex, TableEdge>> getTableFieldBloodChain(TableVertex vertex) {
        return this.getTableFieldBloodChain(vertex, true);
    }

    /**
     * 根据表节点，追溯其字段血缘
     * @param verte	字段节点
     * @param forward
     * @return	字段血缘
     */
    public Map<TableVertex, AsSubgraph<TableVertex, TableEdge>> getTableFieldBloodChain(TableVertex vertex, boolean forward) {
        Map<TableVertex, AsSubgraph<TableVertex, TableEdge>> map = new LinkedHashMap<>();
        if(TableLable.LABLE_TABLE.equalsIgnoreCase(vertex.getLable())) {
            DefaultDirectedGraph<TableVertex, TableEdge> fieldsGraph = this.getTableFieldBloodGraph(forward);
            if(fieldsGraph.containsVertex(vertex)) {
                //获取表的属性关系
                Set<TableEdge> fieldEdges = new HashSet<>();
                for(TableEdge edge : fieldsGraph.edgesOf(vertex)) {
                    if(TableRelation.REL_TABLE_TO_FIELD.equalsIgnoreCase(edge.getRelation())) {
                        fieldEdges.add(edge);
                    }
                }
                //获取表的属性节点
                Set<TableVertex > fieldVertexs = new HashSet<>();
                for(TableEdge edge : fieldEdges) {
                    TableVertex vertex3 = fieldsGraph.getEdgeSource(edge);
                    if(TableLable.LABLE_FIELD.equalsIgnoreCase(vertex3.getLable())) {
                        fieldVertexs.add(vertex3);
                    }
                    TableVertex vertex4 = fieldsGraph.getEdgeTarget(edge);
                    if(TableLable.LABLE_FIELD.equalsIgnoreCase(vertex4.getLable())) {
                        fieldVertexs.add(vertex4);
                    }
                }
                //获取血缘集合
                for(TableVertex tableVertex : fieldVertexs) {
                    map.put(tableVertex, this.getSingleFieldBloodChain(tableVertex, forward));
                }
            }
        }
        return map;
    }

}
