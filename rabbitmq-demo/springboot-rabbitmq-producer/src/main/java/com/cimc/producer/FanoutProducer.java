package com.cimc.producer;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author chenz
 * @create 2019-09-19 16:04
 */
@Component
public class FanoutProducer {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    /**
     * 发送消息
     * @param queueName
     * @param msg
     */
    public void send(String queueName, String msg) {
        System.out.println(msg);
        rabbitTemplate.convertAndSend(queueName, msg);
    }
}
