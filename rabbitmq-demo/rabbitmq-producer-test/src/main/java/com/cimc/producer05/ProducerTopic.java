package com.cimc.producer05;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 通配符模式，生产者
 *
 * @author chenz
 * @create 2019-09-19 13:28
 */
public class ProducerTopic {
    /**
     * 声明两个队列绑定到交换机
     */
    private static final String QUEUE_INFO_EMAIL = "queue_info_email";
    private static final String QUEUE_INFO_SMS = "queue_info_sms";

    /**
     * 声明交换机名称，类型为direct
     */
    private static final String EXCHANGE_TOPIC_INFO = "exchange_topic_info";

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

            /**
             * 声明交换机
             * 参数明细
             * param1:交换机名称
             * param2:交换机类型 fanout、topic、direct、headers
             */
            channel.exchangeDeclare(EXCHANGE_TOPIC_INFO, BuiltinExchangeType.TOPIC);


            /**
             * 声明队列，如果Rabbit中没有此队列将自动创建
             * param1:队列名称
             * param2:是否持久化
             * param3:队列是否独占此连接
             * param4:队列不再使用时是否自动删除此队列
             * param5:队列参数
             */
            channel.queueDeclare(QUEUE_INFO_EMAIL, true, false, false, null);
            channel.queueDeclare(QUEUE_INFO_SMS, true, false, false, null);

            //发送邮件消息
            for (int i = 0; i < 10; i++) {
                String message = "email info to user" + i;
                /**
                 * 消息发布方法
                 * param1:Exchange的名称，如果没有指定，则使用Default Exchange,每个队列也会绑定那个默认的交换机
                 *     但是不能显示绑定或解除绑定
                 *     默认的交换机，routingKey等于队列名称
                 * param2:routingKey,消息的路由Key，是用于Exchange（交换机）将消息转发到指定的消息队列
                 * param3:消息包含的属性
                 * param4:消息体
                 */
                channel.basicPublish(EXCHANGE_TOPIC_INFO, "info.email", null, message.getBytes());
                System.out.println("Send Message is:'" + message + "'");
            }
            //发送短信消息
            for (int i = 0; i < 8; i++) {
                String message = "sms info to user" + i;
                channel.basicPublish(EXCHANGE_TOPIC_INFO, "info.sms", null, message.getBytes());
                System.out.println("Send Message is:'" + message + "'");
            }
            //同时发送短信和邮件消息
            for (int i = 0; i < 6; i++) {
                String message = "sms and email info to user" + i;
                channel.basicPublish(EXCHANGE_TOPIC_INFO, "info.email.sms", null, message.getBytes());
                System.out.println("Send Message is:'" + message + "'");
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
