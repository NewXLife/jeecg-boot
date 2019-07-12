package org.jeecg.express;

import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;

public class ExpressTransformAtomMethod {
	
    
    /**
     * 去掉字符串中的数字
     * @param str 待处理的字符串
     * @return 处理以后的字符串
     */
    public static String replaceNumeric(String str) {
        if (str == null) {
            return null;
        }
        return str.replaceAll("\\d+", "").trim();
    }
    
    /**
     * 去掉字符串中的字母
     * @param str 待处理的字符串
     * @return 处理以后的字符串
     */
    public static String replaceLetter(String str) {
        if (str == null) {
            return null;
        }
        return str.replaceAll("[a-z]|[A-Z]+", "").trim();
    }
	
    /**
     * 转换为Integer类型
     * @param val 待处理的字符串
     * @return Integer
     */
    public static Integer toInteger(Object val) {
    	if(val == null){
    		return null;
    	}
    	Double temp = Double.parseDouble(val.toString().trim());
		if(Integer.MIN_VALUE <= temp && temp <= Integer.MAX_VALUE){
			return temp.intValue();
		}
		else{
			return Integer.parseInt(val.toString().trim());
		}
    }
    
    /**
     * 转换为Long类型
     * @param val 待处理的字符串
     * @return Long
     */
    public static Long toLong(Object val) {
    	if(val == null){
    		return null;
    	}
    	Double temp = Double.parseDouble(val.toString().trim());
		if(Long.MIN_VALUE <= temp && temp <= Long.MAX_VALUE){
			return temp.longValue();
		}
		else{
			return Long.parseLong(val.toString());
		}
    }

    /**
     * 转换为Float类型
     * @param val 待处理的字符串
     * @return Float
     */
    public static Float toFloat(Object val) {
    	if(val == null){
    		return null;
    	}
    	else{
    		return Float.parseFloat(val.toString().trim());
    	}
    }
    
    /**
     * 转换为Double类型
     * @param val 待处理的字符串
     * @return Double
     */
    public static Double toDouble(Object val) {
    	if(val == null){
    		return null;
    	}
    	else{
    		return Double.parseDouble(val.toString());
    	}
    }
    
    /**
     * 转换为String类型
     * @param val 待处理的字符串
     * @return String
     */
    public static String toString(Object val) {
    	return String.valueOf(val);
    }
    
    /**
     * 获取字符trim
     * @param str 待处理的字符串
     * @return trimz值
     */
    public static String trim(String str) {
        return str == null ? null : str.trim();
    }
    
    /**
     * 获取字符长度
     * @param str 待处理的字符串
     * @return 长度
     */
    public static Integer length(Object obj) {
    	if(null == obj){
    		return 0;
    	}
    	//String
    	if (obj instanceof String){
    		return String.valueOf(obj).length();
    	}
    	// Map
    	if (obj instanceof Map){
    		return ((Map<?,?>)obj).size();
    	}
    	// List
    	if (obj instanceof List){
    		return ((List<?>)obj).size();
    	}
    	// Array
        if (obj.getClass().isArray()) {
        	return Array.getLength(obj);
        }
        // other
        return -1;
    }
    
    /**
     * 获取字符拆分
     * @param str 待处理的字符串
     * @return 拆分数组
     */
    public static String[] split(String str, String regex) {
    	if(null == str){
    		return new String[0];
    	}
    	if(null == regex){
    		regex = ""; 
    	}
    	return str.split(regex);
    }

}
