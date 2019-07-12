package org.jeecg.express;

import com.ql.util.express.DefaultContext;
import com.ql.util.express.IExpressContext;


public class ExpressFactory
{
	
	private static final String EXPRESS_DATA_ROW 	= "row";
	private static final String EXPRESS_DATA_PARAMS = "params";
	
	/**
	 * 	构建模型
	 * @param model	验证模型
	 * @return	IExpressContext
	 */
	private static IExpressContext<String,Object> getContext(ExpressModel model){
		IExpressContext<String,Object> expressContext = new DefaultContext<String,Object>();
		expressContext.put(EXPRESS_DATA_ROW, 	model.getRow());
		expressContext.put(EXPRESS_DATA_PARAMS, model.getParams());
		return expressContext;
	}
	
	
	/**
	 * 	装换数据模型
	 * @param model 验证模型
	 * @return Object
	 * @throws Exception
	 */
	public static Object transform(ExpressModel model) throws Exception{
		Object res = ExpressRunnerLoader.instance().loader()
				.execute(model.getExpress(), getContext(model), null, true, false);
		return res;
	}
	
}
