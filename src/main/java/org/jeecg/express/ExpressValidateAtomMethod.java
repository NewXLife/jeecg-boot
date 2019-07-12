package org.jeecg.express;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ExpressValidateAtomMethod {
	
	/**
	 * 判断字符串是否为空
	 * @param cs 字符串
	 * @return
	 */
	public static boolean isEmpty(String cs) {
        return cs == null || cs.length() == 0;
    }
    
	/**
	 * 判断字符串是否不为空
	 * @param cs 字符串
	 * @return
	 */
    public static boolean isNotEmpty(String cs) {
        return !isEmpty(cs);
    }
    
    /**
     * 判断字符串是否为空或空白字符
     * @param cs 字符串
     * @return
     */
    public static boolean isBlank(String cs) {
        int strLen;
        if (cs == null || (strLen = cs.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (Character.isWhitespace(cs.charAt(i)) == false) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * 判断字符串是否不为空或空白字符
     * @param cs 字符串
     * @return
     */
    public static boolean isNotBlank(String cs) {
        return !isBlank(cs);
    }
	
	/**
     * 根据Unicode编码完美的判断中文汉字和符号
     * @param c 字符
     * @return 返回是否是中文字符
     */
    public static boolean isChineseChar(Character c) {
    	if(null == c){ 
    		return false; 
    	}
        if ((c >= 0x4e00) && (c <= 0x9fbb)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 判断字符串中是否全是中文
     * @param str 字符串
     * @return 含有中文则返回true
     */
    public static boolean isAllChinese(String str) {
    	if(null == str){
    		return false;
    	}
        char[] ch = str.toCharArray();
        for (int i = 0; i < ch.length; i++) {
            char c = ch[i];
            if (!isChineseChar(c)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 判断字符串为中文,但排除others
     * @param str    字符串
     * @param others 需要排除的字符串
     * @return 满足条件返回true
     */
    public static boolean isAllChineseBySkip(String str, Character[] others) {
    	if(null == str){
    		return false;
    	}
        //去掉排除的字符串
        if (others != null && others.length > 0) {
            for (char oc : others) {
                String tmp = String.valueOf(oc);
                str = str.replace(tmp, "");
            }
        }
        //验证
        return isAllChinese(str);
    }
    
    /**
     * 判断字符串中是否含有中文
     * @param str 字符串
     * @return 含有中文则返回true
     */
    public static boolean hasChinese(String str) {
        return hasLeastChinese(str, 1);
    }

    /**
     * 判断字符串中是否含有中文
     * @param str 字符串
     * @param min 最少出现中文的次数
     * @return 含有中文则返回true
     */
    public static boolean hasLeastChinese(String str, Integer min) {
    	if(null == str){
    		return false;
    	}
    	if(min == null){
    		min = 0;
    	}
        char[] ch = str.toCharArray();
        for (int i = 0; i < ch.length; i++) {
            char c = ch[i];
            if (isChineseChar(c)) {
                min = min - 1;
                if (min <= 0) {
                    return true;
                }
            }
        }
        return false;
    }
    
    
    /**
     * 根据Unicode编码完美的判断英文字符
     * @param c 字符
     * @return 返回是否是英文字符
     */
    public static boolean isLetterChar(Character c) {
    	if(null == c){ 
    		return false; 
    	}
        if ((c >= 0x41) && (c <= 0x5a)) {
            return true;
        }
        else if((c >= 0x61) && (c <= 0x7a)){
        	return true;
        }
        else {
            return false;
        }
    }
    
    /**
     * 判断字符串是否为字母
     * @param str 字符串
     * @return 是字母则返回true
     */
    public static boolean isAllLetter(String str) {
        return isMatcher(str, "^([a-z]|[A-Z])+$");
    }
    
    /**
     * 判断字符串是否存在字母
     * @param str 字符串
     * @return 是字母则返回true
     */
    public static boolean hasLetter(String str) {
    	return hasLeastLetter(str, 1);
    }
    
    /**
     * 判断字符串是否存在字母
     * @param str 字符串
     * @param min 最少出现的次数
     * @return 是字母则返回true
     */
    public static boolean hasLeastLetter(String str, Integer min) {
    	if(null == str || str.isEmpty()){
    		return false;
    	}
    	if(min == null){
    		min = 0;
    	}
        //判断是否为字母
        char[] ch = str.toCharArray();
        for (int i = 0; i < ch.length; i++) {
            if (isLetterChar(ch[i])) {
                min = min - 1;
                if (min <= 0) {
                    return true;
                }
            }
        }
        return false;
    }
    
    
    /**
     * 判断字符串是否为纯数字
     * @param str 字符串
     * @return 是数字则返回true
     */
    public static boolean isNumericChar(Character c)  {
    	if(null == c){
    		return false;
    	}
        if (!Character.isDigit(c)) {
            return false;
        }
        return true;
    }
    
	
    /**
     * 判断字符串是否为纯数字
     * @param str 字符串
     * @return 是数字则返回true
     */
    public static boolean isAllNumericStr(String str) {
    	if(null == str){
    		return false;
    	}
        for (int i = str.length(); --i >= 0; ) {
            if (!Character.isDigit(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 判断字符串是否为固定长度的数字
     * @param str    字符串
     * @param length 约束的长度
     * @return 满足条件返回true
     */
    public static boolean isAllNumericStrAndLen(String str, Integer length) {
    	if(null == length){
    		length = 0;
    	}
        if (isAllNumericStr(str) && str.length() == length) {
            return true;
        } else {
            return false;
        }
    }
    
    /**
     * 判断字符串是否存在数字
     * @param str 字符串
     * @return 是数字则返回true
     */
    public static boolean hasNumericStr(String str) {
    	return hasLeastNumericStr(str, 1);
    }
    
    /**
     * 判断字符串是否存在数字
     * @param str 字符串
     * @param min 最少出现的次数
     * @return 是数字则返回true
     */
    public static boolean hasLeastNumericStr(String str, Integer min) {
    	if(null == str || str.isEmpty()){
    		return false;
    	}
    	if(min == null){
    		min = 0;
    	}
        //判断是否为字母
        char[] ch = str.toCharArray();
        for (int i = 0; i < ch.length; i++) {
            if (isNumericChar(ch[i])) {
                min = min - 1;
                if (min <= 0) {
                    return true;
                }
            }
        }
        return false;
    }
    
    
    
    /**
     * 包含
     * @param str  当前字符串
     * @param keys 需要包含的key
     * @return 若str中包含key串，则返回true，否则返回false.
     */
    public static boolean isContains(String str, String key) {
    	if(str == null || key == null){
    		return false;
    	}
        return str.contains(key);
    }
	

    /**
     * 包含任意项
     * @param str  当前字符串
     * @param keys 需要包含的key
     * @return 若str中包含keys中的任意项，则返回true，否则返回false.
     */
    public static boolean isContainsAny(String str, String keys[]) {
    	if(str == null || keys == null){
    		return false;
    	}
        for (String key : keys) {
            if (str.contains(key)) {
                return true;
            }
        }
        return false;
    }
	
    /**
     * 包含全部项
     * @param str  当前字符串
     * @param keys 需要包含的key
     * @return 若str中包含keys中的全部项，则返回true，否则返回false.
     */
    public static boolean isContainsAll(String str, String keys[]) {
    	if(str == null || keys == null){
    		return false;
    	}
        for (String key : keys) {
            if (!str.contains(key)) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * 正则表达式验证
     * @param str	当前字符串
     * @param reg	正则表达式
     * @return
     */
    public static boolean isMatcher(String str, String reg){
    	if(str == null || reg == null){
    		return false;
    	}
    	Pattern pattern = Pattern.compile(reg, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(str);
        return matcher.matches();
    }
    
    /**
     * 验证是否为日期格式
     * @param str	待验证字符串
     * @return 满足条件返回true
     */
    public static boolean isDate(String str) {
        String pattern = null;
        if (isMatcher(str, "^\\d{4}-\\d{1,2}-\\d{1,2}$")) {
            pattern = "yyyy-MM-dd";
        }
        if (isMatcher(str, "^\\d{4}年\\d{1,2}月\\d{1,2}日$")) {
            pattern = "yyyy年MM月dd日";
        }
        if (isMatcher(str, "^\\d{4}/\\d{1,2}/\\d{1,2}$")) {
            pattern = "yyyy/MM/dd";
        }
        if (isMatcher(str, "^\\d{4}\\d{2}\\d{2}$")) {
            pattern = "yyyyMMdd";
        }
        if (isMatcher(str, "^\\d{4}.\\d{1,2}.\\d{1,2}$")) {
            str = str.replace('.', '-');
            pattern = "yyyy-MM-dd";
        }
        if (pattern != null) {
            SimpleDateFormat sp = new SimpleDateFormat(pattern);
            try {
            	sp.setLenient(false);
                sp.parse(str);
            } catch (ParseException e) {
                return false;
            }
            return true;
        } else {
            return false;
        }
    }
    
    /**
     * 验证是否为日期格式
     * @param str	待验证字符串
     * @param pattern 规则表达式
     * @return 满足条件返回true
     */
    public static boolean isDateForPattern(String str, String pattern) {
        if (pattern != null) {
            SimpleDateFormat sp = new SimpleDateFormat(pattern);
            try {
            	sp.setLenient(false);
                sp.parse(str);
            } catch (ParseException e) {
                return false;
            }
            return true;
        } else {
            return false;
        }
    }
	
    /**
     * 对象对比
     * @param str1 str1
     * @param str2 str2
     * @return 是否相同
     */
    public static boolean equals(Object str1, Object str2) {
        if (str1 == null && str2 == null) {
            return true;
        }
        if (str1 != null) {
            return str1.equals(str2);
        } else {
            return str2.equals(str1);
        }
    }
    
}
