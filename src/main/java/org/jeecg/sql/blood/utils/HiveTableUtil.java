package org.jeecg.sql.blood.utils;

import com.mxgraph.layout.hierarchical.mxHierarchicalLayout;
import com.mxgraph.layout.mxParallelEdgeLayout;
import com.mxgraph.swing.mxGraphComponent;
import org.apache.hadoop.hive.ql.lib.*;
import org.apache.hadoop.hive.ql.parse.*;
import org.jeecg.sql.blood.enums.CleanConstant;
import org.jeecg.sql.blood.vo.HIveSqlTableEdge;
import org.jeecg.sql.blood.vo.HIveSqlTableVertex;
import org.jeecg.sql.blood.vo.HiveSqlAnalyzeResult;
import org.jgrapht.Graph;
import org.jgrapht.ext.JGraphXAdapter;
import org.jgrapht.graph.AbstractBaseGraph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import java.io.*;
import java.util.*;

public class HiveTableUtil {
	
	private static final Logger logger = LoggerFactory.getLogger(HiveTableUtil.class);
	
	/**变量替换正则*/
	private static final String regex_param = "\\$\\{.*?\\}|\\$[a-zA-Z_]+[a-z-A-Z_0-9]+";
	/**排除列替换正则*/
	private static final String regex_exclude = "`\\(.*?\\).+?`";

	private static class HiveTableProcessor implements NodeProcessor
	{
		//当前数据库
		private String thisDatabase = "default";
		//sql中的输入表
		private TreeSet<String> inputTableList = new TreeSet<String>();
		//sql中的输出表
		private TreeSet<String> outputTableList = new TreeSet<String>();

		/**
		 * 实现的逻辑解析函数
		 */
		@Override
		public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs) throws SemanticException {
			ASTNode pt = (ASTNode) nd;
			System.out.println("1=pt="+pt.toStringTree());
			System.out.println("2=pt="+pt.getToken().getText());
			System.out.println("3=pt="+pt.getToken().getType());
			logger.info("1=pt="+pt.toStringTree());
			logger.info("2=pt="+pt.getToken().getText());
			logger.info("3=pt="+pt.getToken().getType());
			switch (pt.getToken().getText()) {
			case "TOK_CREATETABLE":
				outputTableList.add(BaseSemanticAnalyzer.getUnescapedName((ASTNode) pt.getChild(0)));
				break;
			case "TOK_TAB":
				logger.info("TOK_TAB="+pt.toStringTree());
				outputTableList.add(BaseSemanticAnalyzer.getUnescapedName((ASTNode) pt.getChild(0)));
				break;
			case "TOK_SWITCHDATABASE":
				this.thisDatabase = BaseSemanticAnalyzer.getUnescapedName((ASTNode) pt.getChild(0));
				break;
			case "TOK_TABREF":
				logger.info("TOK_TABREF="+pt.toStringTree());
				ASTNode tabTree = (ASTNode) pt.getChild(0);
				String table_name = (tabTree.getChildCount() == 1)
						? this.thisDatabase + "." + BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0))
						: BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) + "."
								+ tabTree.getChild(1);
				inputTableList.add(table_name);
				break;
				default:
					throw new IllegalStateException("Unexpected value: " + pt.getToken().getText());
			}
			return null;
		}
		
		public TreeSet<String> getInputTableList() {
			return inputTableList;
		}

		public TreeSet<String> getOutputTableList() {
			return outputTableList;
		}
		
		public void cleanTableList() {
			inputTableList.clear();
			outputTableList.clear();
		}
	}
	
	/**
	 * sql清洗【去掉末尾的分号，空白字符等】
	 * @param sql 待待处理的sql
	 * @return 清洗后的sql
	 */
	private static String cleanSql(String sql) {
		if(sql.endsWith(";")) {
			sql = sql.substring(0, sql.length()-1);
		}
		sql = sql + " ";
		//参数信息替换
		sql = sql.replaceAll(regex_param, "1024");
		//sql = sql.replaceAll("\\s\\$\\{.*?\\}\\s", " \"param\" ");
		//替换排除列：select `(name|id|pwd)?+.+` from table
		sql = sql.replaceAll(regex_exclude, "*");
		//替换`
		sql = sql.trim().replace("`", "");
		System.out.println(sql);
		return sql;
	}
	
	/**
	 * 获取给与的sql列表的表输入结构
	 * @param querys	所有的sql
	 * @throws SemanticException
	 * @throws ParseException
	 * @throws IOException 
	 */
	public static HiveSqlAnalyzeResult getTableTree(String ... querys) throws SemanticException, ParseException, IOException {
		//修正后的HQL
		List<String> hsqls = new ArrayList<>();
		//构建的图 
		AbstractBaseGraph<HIveSqlTableVertex, HIveSqlTableEdge> dag = new DefaultDirectedGraph<HIveSqlTableVertex, HIveSqlTableEdge>(HIveSqlTableEdge.class);
		//SQL解析工具
		HiveTableProcessor hiveTableProcessor = new HiveTableProcessor();
		//执行完毕，返回信息
		hsqls.add(String.format("select %s('${%s}', 'null', '%s');", CleanConstant.HIVE_MONITOR_UDF_NAME, CleanConstant.HIVE_MONITOR_EXECUTE_ID, CleanConstant.JOB_TASK_START));
		//循环处理SQL
		int i = 0;
		for (String query : querys) {
			//空值不处理
			if(query == null || query.trim().isEmpty()) {
				continue;
			}
			else{
				query = query.trim();
			}
			//随机任务ID
			String run_stageId = UUID.randomUUID().toString().replace("-", "");
			String set_stageId = "_" + run_stageId;
			//参数设置不参与解析, //添加上报接口
			if(query.length() > 3 && (query.substring(0, 3).equalsIgnoreCase("set") || query.substring(0, 3).equalsIgnoreCase("use"))) {
				hsqls.add(String.format("select %s('${%s}', '%s', '%s');", CleanConstant.HIVE_MONITOR_UDF_NAME, CleanConstant.HIVE_MONITOR_EXECUTE_ID, set_stageId, CleanConstant.JOB_STAGE_START));
				hsqls.add(query);
				hsqls.add(String.format("select %s('${%s}', '%s', '%s');", CleanConstant.HIVE_MONITOR_UDF_NAME, CleanConstant.HIVE_MONITOR_EXECUTE_ID, set_stageId, CleanConstant.JOB_STAGE_END));
				continue;
			}
			else{
				hsqls.add(String.format("select %s('${%s}', '%s', '%s');", CleanConstant.HIVE_MONITOR_UDF_NAME, CleanConstant.HIVE_MONITOR_EXECUTE_ID, run_stageId, CleanConstant.JOB_STAGE_START));
				hsqls.add(query);
				hsqls.add(String.format("select %s('${%s}', '%s', '%s');", CleanConstant.HIVE_MONITOR_UDF_NAME, CleanConstant.HIVE_MONITOR_EXECUTE_ID, run_stageId, CleanConstant.JOB_STAGE_END));
			}
			//生成语法树
			ParseDriver pd = new ParseDriver();
			ASTNode tree = pd.parse(cleanSql(query));
			while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
				tree = (ASTNode) tree.getChild(0);
			}
			System.out.println(tree.toStringTree());
			logger.info("xxx1tree="+tree.toStringTree());
			//清除内部表信息
			hiveTableProcessor.cleanTableList();
			//解析语法树，获取结果
			Map<Rule, NodeProcessor> rules = new LinkedHashMap<Rule, NodeProcessor>();
			Dispatcher disp = new DefaultRuleDispatcher(hiveTableProcessor, rules, null);
			GraphWalker ogw = new DefaultGraphWalker(disp);
			List<Node> topNodes = new ArrayList<Node>();
			topNodes.add(tree);
			ogw.startWalking(topNodes, null);
			//获取输入表与输出表
			TreeSet<String> inputTables = hiveTableProcessor.getInputTableList();
			TreeSet<String> outputTables = hiveTableProcessor.getOutputTableList();
			
			System.out.println(inputTables);
			System.out.println(outputTables);
			logger.info("inputTables="+inputTables.toString());
			logger.info("outputTables="+outputTables.toString());
			
			//不全为空则处理
			if(!inputTables.isEmpty() || !outputTables.isEmpty()) {
				//计数器
				if(!inputTables.isEmpty() && !outputTables.isEmpty()) {
					++ i;
				}
				//处理表血缘图
				for(String table : inputTables) {
					dag.addVertex(new HIveSqlTableVertex(table));
				}
				for(String table : outputTables) {
					dag.addVertex(new HIveSqlTableVertex(table));
				}
				for(String start : inputTables) {
					for(String end : outputTables) {
						dag.addEdge(new HIveSqlTableVertex(start), new HIveSqlTableVertex(end), new HIveSqlTableEdge(run_stageId, i, 0));
					}
				}
			}
		}
		//执行完毕，返回信息
		hsqls.add(String.format("select %s('${%s}', 'null', '%s');", CleanConstant.HIVE_MONITOR_UDF_NAME, CleanConstant.HIVE_MONITOR_EXECUTE_ID, CleanConstant.JOB_TASK_END));
		
		//处理sql
		List<String> hqls = new ArrayList<>();
		for(String hql : hsqls) {
			if(!hql.trim().endsWith(";")) {
				hql = hql + ";";
			}
			hqls.add(hql);
		}
		
		//返回处理结果
		return new HiveSqlAnalyzeResult(hqls, dag);
	}
	
	/**
	 * 将对修序列化存储
	 * @param obj
	 * @return 序列化后的字符串
	 * @throws IOException 
	 */
	public static <T extends Serializable> byte[] graph2Byte(Object graph) throws IOException {
		if(graph == null) {
			return null;
		}
		ByteArrayOutputStream baops = null;
		ObjectOutputStream oos = null;
		try {
			baops = new ByteArrayOutputStream();
			oos = new ObjectOutputStream(baops);
			oos.writeObject(graph);
			return baops.toByteArray();
		} catch (IOException e) {
			throw e;
		} finally {
			try {
				if (oos != null) {
					oos.close();
				}
				if (baops != null) {
					baops.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 反序列图
	 * @param objBody
	 * @param clazz
	 * @return
	 * @throws Exception 
	 */
	@SuppressWarnings("unchecked")
	public static AbstractBaseGraph<HIveSqlTableVertex, HIveSqlTableEdge> byte2Graph(byte[] bytes) throws IOException{
		ObjectInputStream ois = null;
		AbstractBaseGraph<HIveSqlTableVertex, HIveSqlTableEdge> graph = null;
		try {
			ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
			return AbstractBaseGraph.class.cast(ois.readObject()); 
		} catch (IOException e) {
			throw e;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			try {
				if (ois != null) {
					ois.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return graph;
	}
	
	public static void show(Graph<HIveSqlTableVertex, HIveSqlTableEdge> graph) {
		JGraphXAdapter<HIveSqlTableVertex, HIveSqlTableEdge> graphx= new JGraphXAdapter<>(graph);
		mxGraphComponent graphComponent = new mxGraphComponent(graphx);
        JFrame frame = new JFrame();
        frame.getContentPane().add(graphComponent);
        new mxHierarchicalLayout(graphx).execute(graphx.getDefaultParent());
        new mxParallelEdgeLayout(graphx).execute(graphx.getDefaultParent());
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.setSize(800, 600);
		frame.setVisible(true);
	}
}
