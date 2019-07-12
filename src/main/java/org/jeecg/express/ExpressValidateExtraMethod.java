package org.jeecg.express;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ExpressValidateExtraMethod {
	
	public static final List<String> BBD_PROVINCE_VALUES = Collections.synchronizedList(
			Arrays.asList(
					"安徽|北京|福建|甘肃|广东|广西|贵州|海南|河北|河南|黑龙江|湖北|湖南|吉林|江苏|江西|辽宁|"+ 
					"内蒙古|宁夏|青海|山东|陕西|上海|四川|天津|西藏|新疆|云南|浙江|重庆|山西|香港|澳门|台湾"
			.split("\\|")));
	
	/**
	 * 省份验证
	 * @param str	待验证字符串
	 * @return	是否为省份
	 */
	public static boolean isProvince(String str) {
		return BBD_PROVINCE_VALUES.contains(str);
	}
	
	
	/**
	 * 字母、数字（或）
	 * @param str
	 * @return
	 */
	public static boolean isContainsLetterOrNumeric(String str){
		if(ExpressValidateAtomMethod.hasLetter(str)){
			return true;
		}
		if(ExpressValidateAtomMethod.hasNumericStr(str)){
			return true;
		}
		return false;
	}
	
	
	/**
	 * 包含中文、数字（或）
	 * @param str
	 * @return
	 */
	public static boolean isContainsChineseOrNumer(String str){
		if(ExpressValidateAtomMethod.hasChinese(str)){
			return true;
		}
		if(ExpressValidateAtomMethod.hasNumericStr(str)){
			return true;
		}
		return false;
	}
	
	/**
	 * 包含中文、字母（或）
	 * @param str
	 * @return
	 */
	public static boolean isContainsChineseOrLetter(String str){
		if(ExpressValidateAtomMethod.hasChinese(str)){
			return true;
		}
		if(ExpressValidateAtomMethod.hasLetter(str)){
			return true;
		}
		return false;
	}
	
	/**
	 * 包含中文、字母、数字（或）
	 * @param str
	 * @return
	 */
	public static boolean isContainsChineseOrLetterOrNumeric(String str){
		if(ExpressValidateAtomMethod.hasChinese(str)){
			return true;
		}
		if(ExpressValidateAtomMethod.hasLetter(str)){
			return true;
		}
		if(ExpressValidateAtomMethod.hasNumericStr(str)){
			return true;
		}
		return false;
	}
	
	
	/**
	 * 包含字母、数字（且）
	 * @param str
	 * @return
	 */
	public static boolean isContainsLetterAndNumeric(String str){
		if(ExpressValidateAtomMethod.hasLetter(str) && ExpressValidateAtomMethod.hasNumericStr(str)){
			return true;
		}
		else{
			return false;
		}
	}
	
	/**
	 * 包含中文、字母、数字（且）
	 * @param str
	 * @return
	 */
	public static boolean isContainsChineseAndLetter(String str){
		if(ExpressValidateAtomMethod.hasChinese(str) && ExpressValidateAtomMethod.hasLetter(str)){
			return true;
		}
		else{
			return false;
		}
	}
	
	/**
	 * 包含中文、字母、数字（且）
	 * @param str
	 * @return
	 */
	public static boolean isContainsChineseAndNumeric(String str){
		if(ExpressValidateAtomMethod.hasChinese(str) && ExpressValidateAtomMethod.hasNumericStr(str)){
			return true;
		}
		else{
			return false;
		}
	}
	
	/**
	 * 包含中文、字母、数字（且）
	 * @param str
	 * @return
	 */
	public static boolean isContainsChineseAndLetterAndNumeric(String str){
		if(ExpressValidateAtomMethod.hasChinese(str) && ExpressValidateAtomMethod.hasLetter(str) && ExpressValidateAtomMethod.hasNumericStr(str)){
			return true;
		}
		else{
			return false;
		}
	}
	
}
