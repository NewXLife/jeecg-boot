package org.jeecg.modules.rabbit.mq;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class Producer
{
    private static final String EXCHANGE = "exchange";

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Value("${spring.rabbitmq.queuename.fannotice}")
    private String queueName;

    /**
     * 发送消息，不需要实现任何接口，供外部调用
     */
    public void producer(String msg)
    {
        rabbitTemplate.convertAndSend(EXCHANGE, msg);
    }
}
