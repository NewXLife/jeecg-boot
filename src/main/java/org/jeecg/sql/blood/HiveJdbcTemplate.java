package org.jeecg.sql.blood;

import org.jeecg.sql.blood.model.MetaInfo;
import org.jeecg.sql.blood.utils.MetaInfoUtil;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HiveJdbcTemplate extends JdbcTemplate{
	
	public HiveJdbcTemplate() {
		super();
	}
	
	public HiveJdbcTemplate(DataSource dataSource) {
		super(dataSource);
	}

	public HiveJdbcTemplate(DataSource dataSource, boolean lazyInit) {
		super(dataSource, lazyInit);
	}

	/**
	 * 	解析元数据信息
	 * @param database
	 * @param table
	 * @return
	 */
	public MetaInfo getMetaInfo(String database, String table) {
		String sql = MetaInfoUtil.getMetaSql(database, table);
		List<Map<String, Object>>  res = this.queryForList(sql);
		return MetaInfoUtil.getMetaInfo(res);
	}
	
	/**
	 * 	获取当前sql的执行计划	
	 * @param sql	hive sql
	 * @return	执行计划
	 */
	public String getExplain(String sql) {
		if(sql ==null || sql.trim().isEmpty() || sql.trim().toLowerCase().startsWith("set") || sql.trim().toLowerCase().startsWith("add")) {
			return "";
		}
		List<String> explains = this.queryForList("explain " +sql, String.class);
		StringBuffer sb = new StringBuffer();
		for(String line : explains) {
			sb.append(line).append("\n");
		}
		return sb.toString();
	}
	
	/**
	 * 	获取当前sql的执行计划	
	 * @param sqls	hive sql
	 * @return	执行计划
	 */
	public List<String> getExplain(List<String> sqls) {
		List<String> explains= new ArrayList<>();
		for(String sql : sqls) {
			explains.add(this.getExplain(sql));
		}
		return explains;
	}
	
	
//	/**
//	 * 	分片数据
//	 * @param sql
//	 * @param callBack
//	 */
//	public void shardingCallBack(String sql, ShardingToMapCallBack callBack) {
//		this.query(sql, new ShardingToMapResultSetExtractor(callBack));
//	}
//
//	/**
//	 * 	分片数据
//	 * @param sql
//	 * @param callBack
//	 */
//	public void shardingCallBack(String sql, ShardingToArrayCallBack callBack) {
//		this.query(sql, new ShardingToArrayResultSetExtractor(callBack));
//	}
//
//	/**
//	 * 执行SQl并回调日志信息
//	 * @param sql
//	 * @param callBack
//	 * @return
//	 * @throws SQLException
//	 */
//	public boolean execute(String sql, ExecuteLogCallBack callBack) throws SQLException {
//		Statement stmt = this.getDataSource().getConnection().createStatement();
//		if(callBack != null) {
//			new ExecuteLogExtractor((HiveStatement) stmt, callBack).start();
//		}
//		return stmt.execute(sql);
//	}
//
//	/**
//	 *	 执行SQl并回调日志信息
//	 * 	@param sqls
//	 * 	@param logCallBack
//	 * 	@return
//	 * 	@throws SQLException
//	 */
//	public boolean execute(List<String> sqls, ExecuteLogCallBack logCallBack) throws SQLException {
//		return this.execute(sqls, logCallBack, null);
//	}
//
//	/**
//	 * 	执行SQl并回调日志信息
//	 * 	@param sqls
//	 * 	@param logCallBack
//	 * 	@param numCallBack
//	 * 	@return
//	 * 	@throws SQLException
//	 * @throws TimeoutException
//	 */
//	public boolean execute(List<String> sqls, ExecuteLogCallBack logCallBack, ExecuteNumCallBack numCallBack) throws SQLException {
//		try {
//			return this.execute(sqls, null, logCallBack, numCallBack);
//		} catch (TimeoutException e) {
//			return false;
//		}
//	}
//
//	/**
//	 * 	执行SQl并回调日志信息
//	 * 	@param sqls
//	 *  @param timeout :秒
//	 * 	@param logCallBack
//	 * 	@param numCallBack
//	 * 	@return
//	 * 	@throws SQLException
//	 * @throws TimeoutException
//	 */
//	public boolean execute(List<String> sqls, Long timeout, ExecuteLogCallBack logCallBack, ExecuteNumCallBack numCallBack) throws SQLException, TimeoutException {
//		Statement stmt = this.getDataSource().getConnection().createStatement();
//		ExecuteLogExtractor extractor = null;
//		if(logCallBack != null) {
//			extractor = new ExecuteLogExtractor((HiveStatement) stmt, logCallBack);
//			extractor.start();
//		}
//		if(timeout == null || timeout <= 0) {
//			timeout = Long.MAX_VALUE;
//		}
//		FutureTask<Boolean> futureTask = new FutureTask<>(new Callable<Boolean>() {
//            @Override
//            public Boolean call() throws Exception {
//            	int num = 1;
//        		for(String sql : sqls) {
//        			if(numCallBack != null) {
//        				numCallBack.callBackRun(num, 0, sql);
//        			}
//        			stmt.execute(sql);
//        			if(numCallBack != null) {
//        				numCallBack.callBackRun(num, 1, sql);
//        			}
//        			num ++ ;
//        		}
//                return true;
//            }
//        });
//		try {
//			new Thread(futureTask).start();
//			return futureTask.get(timeout, TimeUnit.SECONDS);
//		}
//		catch (Exception e) {
//			futureTask.cancel(true);
//			if(e instanceof TimeoutException) {
//				throw (TimeoutException)e;
//			}
//			else if (e instanceof SQLException) {
//				throw (SQLException)e;
//			}
//			else {
//				throw new SQLException(e);
//			}
//		}
//		finally {
//			try {
//				if(extractor != null) {
//					//通知日志模块
//					extractor.setEnd(true);
//					//等待日志处理
//					Thread.sleep(6000);
//				}
//				//关闭HiveStatement
//				stmt.close();
//				logger.info("任务结束，关闭HiveStatement");
//			} catch (Exception e) {
//				logger.error("任务结束，关闭HiveStatement异常：", e);
//			}
//		}
//	}
//
//
//	/**
//	 * 查询数据转化为Map的对象
//	 * @param sql
//	 * @param callBack
//	 * @return
//	 * @throws SQLException
//	 */
//	public Map<String, Object> queryForMap(String sql, ExecuteLogCallBack callBack) throws SQLException {
//		Statement stmt = this.getDataSource().getConnection().createStatement();
//		if(callBack != null) {
//			new ExecuteLogExtractor((HiveStatement) stmt, callBack).start();
//		}
//		return ResultSetUtil.toMap(stmt.executeQuery(sql));
//	}
//
//	/**
//	 * 查询数据转化为指定的对象
//	 * @param sql
//	 * @param requiredType
//	 * @param callBack
//	 * @return
//	 * @throws SQLException
//	 */
//	public <T> T queryForObject(String sql, Class<T> requiredType, ExecuteLogCallBack callBack) throws SQLException {
//		Statement stmt = this.getDataSource().getConnection().createStatement();
//		if(callBack != null) {
//			new ExecuteLogExtractor((HiveStatement) stmt, callBack).start();
//		}
//		return ResultSetUtil.toObject(stmt.executeQuery(sql), requiredType);
//	}
//
//	/**
//	 * 执行SQl并回调日志信息
//	 * @param sql
//	 * @param callBack
//	 * @return
//	 * @throws SQLException
//	 */
//	public List<Map<String, Object>> queryForList(String sql, ExecuteLogCallBack callBack) throws SQLException {
//		Statement stmt = this.getDataSource().getConnection().createStatement();
//		if(callBack != null) {
//			new ExecuteLogExtractor((HiveStatement) stmt, callBack).start();
//		}
//		return ResultSetUtil.toListMap(stmt.executeQuery(sql));
//	}
//
//	/**
//	 * 执行SQl并回调日志信息
//	 * @param sql
//	 * @param elementType
//	 * @param callBack
//	 * @return
//	 * @throws SQLException
//	 */
//	public <T> List<T> queryForList(String sql, Class<T> elementType, ExecuteLogCallBack callBack) throws SQLException {
//		Statement stmt = this.getDataSource().getConnection().createStatement();
//		if(callBack != null) {
//			new ExecuteLogExtractor((HiveStatement) stmt, callBack).start();
//		}
//		return ResultSetUtil.toList(stmt.executeQuery(sql), elementType);
//	}
	
}
