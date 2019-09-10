package com.cimc.producer02;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 公平队列测试
 *
 * @author chenz
 * @create 2019-09-08 22:43
 */
public class Producer02 {
    /**
     * 队列名称
     */
    private static final String QUEUE = "HelloWorld";

    public static void main(String[] args) throws IOException, TimeoutException {
        Connection connection = null;
        Channel channel = null;
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("192.168.20.150");
            factory.setPort(5672);
            factory.setUsername("guest");
            factory.setPassword("guest");
            //rabbitmq默认虚拟机名称为"/"，虚拟机相当于一个独立的mq服务器
            factory.setVirtualHost("/");
            //创建与RabbitMQ服务的TCP连接
            connection = factory.newConnection();
            //创建与Exchange的通道，每个连接可以创建多个通道，每个通道代表一个会话任务
            channel = connection.createChannel();

            channel.queueDeclare(QUEUE, true, false, false, null);
            String message = "HelloWorld ";

            for (int i = 0; i < 100; i++) {
                channel.basicPublish("", QUEUE, null, (message + i).getBytes());
                System.out.println("Send Message is:'" + message + i + "'");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (channel != null) {
                channel.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }
}
