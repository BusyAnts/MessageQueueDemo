package com.cimc.producer07;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * AMQP事务机制--生产者
 *
 * @author chenz
 * @create 2019-09-08 22:43
 */
public class ProducerAMQP {
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

            // 将当前管道设置为txSelect，将当前channel设置为transaction模式，开启事务
            channel.txSelect();
            String message = "HelloWorld" + System.currentTimeMillis();
            channel.basicPublish("", QUEUE, null, message.getBytes());
            int i = 1 / 0;
            System.out.println("Send Message is:'" + message + "'");
            channel.txCommit();
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("消息进行回滚操作");
            channel.txRollback();
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
