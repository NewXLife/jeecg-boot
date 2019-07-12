package org.jeecg.express;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ExpressModel implements Serializable{
	
	private static final long serialVersionUID = -8570990647924537902L;
	
	private	String express;
	private Map<String, Object> row = new HashMap<>();
	private Map<String, Object> params = new HashMap<>();
	
	
	public ExpressModel(String express, Map<String, Object> row) {
		this.express = express;
		this.row = row;
	}

	public ExpressModel(String express, Map<String, Object> row, Map<String, Object> params) {
		this.express = express;
		this.row = row;
		this.params = params;
	}
	
	public String getExpress() {
		return express;
	}

	public void setExpress(String express) {
		this.express = express;
	}

	public Map<String, Object> getRow() {
		return row;
	}

	public void setRow(Map<String, Object> row) {
		this.row = row;
	}

	public Map<String, Object> getParams() {
		return params;
	}

	public void setParams(Map<String, Object> params) {
		this.params = params;
	}

	@Override
	public String toString() {
		return "ExpressModel [express=" + express + ", row=" + row + ", params=" + params + "]";
	}
	
}
