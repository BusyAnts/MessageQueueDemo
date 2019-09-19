package com.cimc.producer06;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * @author chenz
 * @create 2019-09-19 14:07
 */
public class ProdecerHeader {
    /**
     * 声明两个队列绑定到交换机
     */
    private static final String QUEUE_INFO_EMAIL = "queue_info_email";
    private static final String QUEUE_INFO_SMS = "queue_info_sms";

    /**
     * 声明交换机名称，类型为headers
     */
    private static final String EXCHANGE_HEADERS_INFO = "exchange_headers_info";

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
            channel.exchangeDeclare(EXCHANGE_HEADERS_INFO, BuiltinExchangeType.HEADERS);


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

            Map<String, Object> headers_email = new Hashtable<String, Object>();
            headers_email.put("info_type", "email");
            Map<String, Object> headers_sms = new Hashtable<String, Object>();
            headers_sms.put("info_type", "sms");

            /**
             * 交换机和队列绑定
             * param1:队列名称
             * param2:交换机名称
             * param3:路由key
             */
            channel.queueBind(QUEUE_INFO_EMAIL, EXCHANGE_HEADERS_INFO, "", headers_email);
            channel.queueBind(QUEUE_INFO_SMS, EXCHANGE_HEADERS_INFO, "", headers_sms);

            //发送邮件消息
            for (int i = 0; i < 10; i++) {
                String message = "email info to user" + i;
                Map<String,Object> headers = new HashMap<>();
                headers.put("info_type","email");
                AMQP.BasicProperties.Builder properties = new AMQP.BasicProperties.Builder();
                properties.headers(headers);
                channel.basicPublish(EXCHANGE_HEADERS_INFO, "", properties.build(), message.getBytes());
                System.out.println("Send Message is:'" + message + "'");
            }
            //发送短信消息
            for (int i = 0; i < 8; i++) {
                String message = "sms info to user" + i;
                Map<String,Object> headers = new HashMap<>();
                headers.put("info_type","sms");
                AMQP.BasicProperties.Builder properties = new AMQP.BasicProperties.Builder();
                properties.headers(headers);
                channel.basicPublish(EXCHANGE_HEADERS_INFO, "", properties.build(), message.getBytes());
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
