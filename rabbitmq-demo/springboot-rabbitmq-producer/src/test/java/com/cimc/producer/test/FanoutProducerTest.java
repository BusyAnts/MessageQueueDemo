package com.cimc.producer.test;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author chenz
 * @create 2019-09-19 16:04
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class FanoutProducerTest {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    private static final String FANOUT_EMAIL_QUEUE = "fanout_email_queue";
    private static final String FANOUT_SMS_QUEUE = "fanout_sms_queue";

    /**
     * 测试发送邮件
     */
    @Test
    public void sendEmailTest() {
        for (int i = 0; i < 5; i++) {
            String message = "email info to user" + i;
            rabbitTemplate.convertAndSend(FANOUT_EMAIL_QUEUE, message);
            System.out.println("Send Message is:'" + message + "'");
        }
    }

    /**
     * 测试发送短信
     */
    @Test
    public void sendSmsTest() {
        for (int i = 0; i < 10; i++) {
            String message = "sms info to user" + i;
            rabbitTemplate.convertAndSend(FANOUT_SMS_QUEUE, message);
            System.out.println("Send Message is:'" + message + "'");
        }
    }
}
