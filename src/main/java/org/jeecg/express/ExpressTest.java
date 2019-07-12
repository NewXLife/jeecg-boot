package org.jeecg.express;


import com.ql.util.express.DefaultContext;
import com.ql.util.express.ExpressRunner;
import com.ql.util.express.IExpressContext;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zcd on 2019-05-23
 */
class ExpressTest
{

    @Test
    void testOne() throws Exception
    {
        ExpressRunner runner = new ExpressRunner();
        DefaultContext<String, Object> context = new DefaultContext<>();
        context.put("a", 1);
        context.put("b", 2);
        String express = "a+b";
        Object r = runner.execute(express, context, null, true, false);
        System.out.println(r);

        DefaultContext<String, Object> row = new DefaultContext<>();
        List<Integer> r1 = new ArrayList<>();
        r1.add(1);
        r1.add(2);
        r1.add(3);
        r1.add(4);

        List<Integer> r2 = new ArrayList<>();
        r2.add(3);
        r2.add(3);
        r2.add(3);
        r2.add(3);
        r2.add(3);
        row.put("c", r1);
        row.put("d", r2);
        Object obj = runner.execute("c+d", row, null, true, false);
        System.out.println(obj);

    }

    @Test
    public void defineFunctionTest() throws Exception
    {
        final String express = "function add(int a,int b){\n" +
                "  return a+b;\n" +
                "};\n" +
                "\n" +
                "function sub(int a,int b){\n" +
                "  return a - b;\n" +
                "};\n" +
                "\n" +
                "a=10;\n" +
                "return add(a,4) + sub(a,9);";
        ExpressRunner runner = new ExpressRunner();
        DefaultContext<String, Object> context = new DefaultContext<>();
        Object r = runner.execute(express, context, null, true, false);
        System.out.println(r);
//        Assertions.assertEquals(15, r);
    }

    @Test
    public void replaceKeywordTest() throws Exception
    {
        ExpressRunner runner = new ExpressRunner();
        runner.addOperatorWithAlias("如果", "if", null);
        runner.addOperatorWithAlias("则", "then", null);
        runner.addOperatorWithAlias("否则", "else", null);
        DefaultContext<String, Object> context = new DefaultContext<>();
        final String express = "如果(1>2){ return 10;} 否则 {return 5;}";
        Object r = runner.execute(express, context, null, true, false);
        System.out.println(r);
//        Assertions.assertEquals(5, r);
    }

    @Test
    public void addOperatorTest() throws Exception
    {
        ExpressRunner runner = new ExpressRunner();
        DefaultContext<String, Object> context = new DefaultContext<>();
        runner.addOperator("join", new JoinOperator());
        Object r = runner.execute("1 join 2 join 3", context, null, false, false);
//        Assertions.assertEquals(Arrays.asList(1,2,3), r);
    }

    @Test
    public void replaceOperatorTest() throws Exception
    {
        ExpressRunner runner = new ExpressRunner();
        DefaultContext<String, Object> context = new DefaultContext<>();
        runner.replaceOperator("+", new JoinOperator());
        Object r = runner.execute("1 + 2 + 3", context, null, false, false);
//        Assertions.assertEquals(Arrays.asList(1,2,3), r);
    }

    @Test
    public void addFunctionTest() throws Exception
    {
        ExpressRunner runner = new ExpressRunner();
        DefaultContext<String, Object> context = new DefaultContext<>();
        runner.addFunction("join", new JoinOperator());
        Object r = runner.execute("join(1, 2, 3)", context, null, false, false);
        //[1, 2, 3]
//        Assertions.assertEquals(Arrays.asList(1,2,3), r);
    }


    static String expressF(String a, int start, int end)
    {
        String res = String.format("function substrDEFINE(String a,int start, int len){\n" +
                "  return a.substring(start, len);\n" +
                "};\n" +
                "return substrDEFINE(%s,%d,%d);", a, start, end);
        return res;
    }

    public static void main(String[] args) throws Exception
    {

        ExpressRunner runner = new ExpressRunner();
        DefaultContext<String, Object> context = new DefaultContext<>();
        context.put("a", 5);
        context.put("b", 10);

        Object r = runner.execute("(a+b)/3", context, null, false, false);
        System.out.println(r);

//        Assertions.assertEquals(Arrays.asList(1,2,3), r);

//        ExpressRunner runner = new ExpressRunner();
//        runner.addMacro("计算平均成绩", "(语文+数学+英语)/3.0");
//        runner.addMacro("是否优秀", "计算平均成绩>90");
//        IExpressContext<String, Object> context = new DefaultContext<>();
//        context.put("语文", 88);
//        context.put("数学", 99);
//        context.put("英语", 95);
//        Boolean result = (Boolean) runner.execute("是否优秀", context, null, false, false);
////        Assertions.assertTrue(result);
//        Object avgScore = runner.execute("计算平均成绩", context, null, false, false);
//        System.out.println(avgScore);
//        System.out.println(result);


        final String express1 = "function add(int a,int b){\n" +
                "  return a+b;\n" +
                "};\n" +
                "\n" +
                "function sub(int a,int b){\n" +
                "  return a - b;\n" +
                "};\n" +
                "\n" +
                "return add(a,b) + sub(a,b);";

        final String express2 = "function substrDEFINE(String a,int start, int len){\n" +
                "  return a.substring(start, len);\n" +
                "};\n" +
                "return substrDEFINE(a,1,3);";
//
//        ExpressRunner runner = new ExpressRunner();
//        DefaultContext<String, Object> context = new DefaultContext<>();
//        context.put("b", 10);
//        context.put("a", "hello-world");
////        context.put("C", 20);
//        Object r = runner.execute(expressF("a",1,5), context, null, false, false);
//        System.out.println(r);

        /**
         * ETL- tools
         */

        /**
         * SUM(Quantity) - 在“数量”字段中添加值。
         * PERCENTILE(Users per day, 50) - 返回“每日用户数”字段的所有值的第50个百分位数。
         * ROUND(Revenue Per User, 0) - 将每用户收入字段四舍五入到0位。
         * SUBSTR(Campaign, 1, 5) - 返回Campaign字段的前5个字符。
         * REGEXP_EXTRACT(Pipe delimited values, '^([a-zA-Z_]*)(\\|)') - 提取管道分隔字符串中的第一个值。
         * DATE_DIFF(Start Date, End Date) - 计算开始日期和结束日期之间的天数。
         * TODATE(concat(Year, '-', Month Number, '-', Day Number), "DEFAULT_DASH", "%Y%m%d") - 通过连接包含有效日期部分的单独字段来创建日期
         */


        /**
         * CASE
         *     WHEN Country IN ("USA","Canada","Mexico")THEN "North America"
         *     WHEN Country IN ("England","France")THEN "Europe"
         *     ELSE "Other"
         * END
         */

        /**
         * CASE WHEN Country ISO Code = "US"  AND Medium = "cpc" THEN "US - Paid" ELSE "other" END
         *
         * CASE WHEN REGEXP_MATCH(Video Title, ".*Google Analytics*")
         * AND is_livestream = TRUE OR Video Length > 120 THEN "GA LIVE or LONG" END
         */


        /**
         * CASE WHEN Medium != "cpc" THEN "free" ELSE "paid" END
         * CASE WHEN Time on Page <= 90 THEN 1 ELSE 0 END
         */

//        ExpressRunner runner = new ExpressRunner();
//        runner.addOperatorWithAlias("`case when`", "if", null);
////        runner.addOperatorWithAlias("THEN", "then", null);
////        runner.addOperatorWithAlias("ELSE", "else", null);
//        DefaultContext<String, Object> context = new DefaultContext<>();
//        context.put("a", 6);
//        context.put("b", 2);
//        final String express = " case   when  a>b THEN a else b";
////        String pattern = "(\\D*)(\\d+)(.*)";
//        String pattern = "(\\w+)(\\s+)when";
//// 创建 Pattern 对象
//        Pattern r = Pattern.compile(pattern);
//// 现在创建 matcher 对象
//        String m = r.matcher(express).replaceFirst("if").toLowerCase();
//        System.out.println(m);
//
//        Object res = runner.execute(m, context, null, true, false);
//        System.out.println(res);
    }

    /**
     * 宏定义
     */
    @Test
    public void macroTest() throws Exception
    {
        ExpressRunner runner = new ExpressRunner();
        runner.addMacro("计算平均成绩", "(语文+数学+英语)/3.0");
        runner.addMacro("是否优秀", "计算平均成绩>90");
        IExpressContext<String, Object> context = new DefaultContext<>();
        context.put("语文", 88);
        context.put("数学", 99);
        context.put("英语", 95);
        Boolean result = (Boolean) runner.execute("是否优秀", context, null, false, false);
//        Assertions.assertTrue(result);
        System.out.println(result);
    }

    /**
     * 绑定java类或者方法
     */
    @Test
    public void bindObjectMethodTest() throws Exception
    {
        ExpressRunner runner = new ExpressRunner();
        DefaultContext<String, Object> context = new DefaultContext<>();

        runner.addFunctionOfClassMethod("取绝对值", Math.class.getName(), "abs",
                new String[]{"double"}, null);
        runner.addFunctionOfClassMethod("转换为大写", BindObjectMethod.class.getName(),
                "upper", new String[]{"String"}, null);
        runner.addFunctionOfServiceMethod("打印", System.out, "println", new String[]{"String"}, null);
        runner.addFunctionOfServiceMethod("contains", new BindObjectMethod(), "anyContains",
                new Class[]{String.class, String.class}, null);
        String exp = "取绝对值(-100);转换为大写(\"hello world\");打印(\"你好吗？\");contains(\"helloworld\",\"aeiou\")";
        Object r = runner.execute(exp, context, null, false, false);
        System.out.println(r);
    }
}