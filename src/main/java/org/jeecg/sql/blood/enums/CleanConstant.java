package org.jeecg.sql.blood.enums;

public class CleanConstant
{
	
	/**样本处理流程枚举**/
	public enum NodeType{CLEAN_COMMON_INIT, CLEAN_COMMON_CHECK, CLEAN_COMMON_EXECUTE, CLEAN_COMMON_ANALYZE, CLEAN_COMMON_DESTROY}
	
	/**样本清洗的子流程**/
	public enum SubDataClean{
		DATA_CLEAN_SUBMIT_ERROR, 			//执行job失败
		DATA_CLEAN_SUBMIT_SUCCESS, 		//执行job成功
		DATA_CLEAN_RUNING_EXECUTE,		//远程任务执行成功
		DATA_CLEAN_RUNING_SUCCESS,
		DATA_CLEAN_RUNING_ERROR
	}
	
	/**hive描点函数**/
	public final static String HIVE_MONITOR_UDF_NAME 	= "ip_sp_udf_tracking";
	
	/**hive描点函数 默认执行id的取值key**/
	public final static String HIVE_MONITOR_EXECUTE_ID = "uuid";

	/**job task 开始标识**/
	public final static String JOB_TASK_START 	= "0";
	/**job task 结束标识**/
	public final static String JOB_TASK_END 	= "1";
	
	/**job stage 开始标识**/
	public final static String JOB_STAGE_START  = "10";
	/**job stage 结束标识**/
	public final static String JOB_STAGE_END 	= "11";
	
	/**样本流程模板标识**/
	public final static String DATA_CLEAN_FLOW_TEMPLATE_PREFIX = "DATA_CLEAN_FLOW_TEMPLATE";
	/**样本流程模板项目ID标识**/
	public final static Long   DATA_CLEAN_FLOW_TEMPLATE_PROJECT_ID1 = -2000L;
	/**样本流程模板项目ID标识**/
	public final static Long   DATA_CLEAN_FLOW_TEMPLATE_MODULE_ID1  = -2000L;
    /**发布状态-正式**/
	public static final String PUBLISH_TYPE_NORMAL = "normal";
	/**发布状态-临时**/
	public static final String PUBLISH_TYPE_TEMPORARY = "temporary";
	/**发布状态-生效**/
	public static final Integer PUBLISH_STATUS_VALID = 1;
	/**发布状态-未生效**/
	public static final Integer PUBLISH_STATUS_INVALID = 0;


}
