package com.cimc.consumer;

import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * 邮件消费者
 *
 * @author chenz
 */
@Component
@RabbitListener(queues = "fanout_email_queue")
public class FanoutEmailConsumer {

    @RabbitHandler
    public void process(String msg) throws Exception {
        System.out.println("接收到的msg:" + msg);
    }
}