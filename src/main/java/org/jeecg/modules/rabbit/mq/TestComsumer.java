package org.jeecg.modules.rabbit.mq;

import org.springframework.stereotype.Service;

@Service
public class TestComsumer
{
    public final String queue = "test";

    //    @RabbitListener(queues = {"one"})
    public void test(String msg)
    {
        System.out.println("im receiver msg = " + msg);
    }

    //    @RabbitListener(queues = {"two"})
    public void test1(String msg)
    {
        System.out.println("im receiver msg = " + msg);
    }


}
