package org.jeecg.modules.rabbit.mq;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;


@Service
public class MailConsumer
{
    @RabbitListener(queues = {"sendmail"})
    public void processMessage(String message){
        try {
             // 通过消息队列sendmail 获取到message 的处理逻辑
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }

}
