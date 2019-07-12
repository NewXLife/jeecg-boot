package org.jeecg.express;

import com.ql.util.express.Operator;

/**
 * Created by zcd on 2019-05-23
 */
public class JoinOperator extends Operator
{
    private static final long serialVersionUID = 5653601029469696306L;

    @Override
    public Object executeInner(Object[] objects)
    {
        java.util.List result = new java.util.ArrayList();

        for (Object object : objects)
        {
            if (object instanceof java.util.List)
            {
                result.addAll(((java.util.List) object));
            } else
            {
                result.add(object);
            }
        }

        return result;
    }
}
