package org.jeecg.express;

/**
 * Created by zcd on 2019-05-23
 */
public class BindObjectMethod
{
    /**
     * 大写
     * @param abc 字符串
     * @return 转换后
     */
    public static String upper(String abc) {
        return abc.toUpperCase();
    }

    /**
     * 任何包含
     * @param str 字符串
     * @param searchStr 查询字符串
     * @return 是否包含
     */
    public boolean anyContains(String str, String searchStr) {

        char[] s = str.toCharArray();
        for (char c : s) {
            if (searchStr.contains(c + "")) {
                return true;
            }
        }
        return false;
    }
}
