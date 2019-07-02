package org.jeecg.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class RabbitMQConfig
{
    @Value("${spring.rabbitmq.host}")
    private String host = "";
    @Value("${spring.rabbitmq.username}")
    private String username = "";
    @Value("${spring.rabbitmq.password}")
    private String password = "";
    final static int port = 5672;

//   use Topic Exchange
    @Value("${spring.rabbitmq.topic.data.notice}")
    private String datatopic;
    @Value("${spring.rabbitmq.topic.data.mail}")
    private String mailqueue;

//   use Fanout Exchange
    @Value("${spring.rabbitmq.queuename.fannotice}")
    private String fannotice;

    @Bean
    Queue queue()
    {
        return new Queue(datatopic, false);
    }

    /**
     * 创建交换机 TopicExchange
     * @return
     */
    @Bean(name = "topic-exchange")
    TopicExchange exchange()
    {
        return new TopicExchange("topic-exchange");
    }//指定从哪个exchange接收数据


    @Bean
    Binding binding(Queue queue, TopicExchange exchange)
    {
        return BindingBuilder.bind(queue).to(exchange).with(datatopic);
    }

    /**
     *创建队列fannotice
     * @return
     */
    @Bean(name = "queueFanoutOne")
    Queue queueFanoutOne()
    {
        return new Queue(fannotice, false);
    }

    /**
     * exchangeFanout
     * 路由广播的形式,将会把消息发给绑定它的全部队列,即便设置了key,也会被忽略.
     * @return
     */

    @Bean
    FanoutExchange exchangeFanout()
    {
        return new FanoutExchange("exchangeFanout");
    }

    @Bean
    Binding bindingFanoutOne(@Qualifier("queueFanoutOne") Queue queueFanoutOne, FanoutExchange exchangeFanout) {
        return BindingBuilder.bind(queueFanoutOne).to(exchangeFanout);
    }

//    自定义connection如下
//    @Bean
//    public ConnectionFactory connectionFactory()
//    {
//        CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
//        connectionFactory.setHost(host);
//        connectionFactory.setPort(port);
//        connectionFactory.setUsername(username);
//        connectionFactory.setPassword(password);
//        connectionFactory.setVirtualHost("/");
//        connectionFactory.setPublisherConfirms(true);
//        connectionFactory.setPublisherConfirms(true);
//        connectionFactory.setPublisherReturns(true);
//        log.info("rabbit-mq info,host= {} port = {}, username= {}, password = {} ", host, port, username, password);
//        return connectionFactory;
//    }
//
//    @Bean
//    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
//    /** 因为要设置回调类，所以应是prototype类型，如果是singleton类型，则回调类为最后一次设置 */
//    public RabbitTemplate rabbitTemplate()
//    {
//        return new RabbitTemplate(connectionFactory());
//    }
//
//    /**
//     * 因为使用了自定义的ConnectionFactory,所以需要定义RabbitAdmin
//     */
//    @Bean(value = "pmsRabbitAdmin")
//    public RabbitAdmin pmsRabbitAdmin()
//    {
//        return new RabbitAdmin(connectionFactory());
//    }
}
