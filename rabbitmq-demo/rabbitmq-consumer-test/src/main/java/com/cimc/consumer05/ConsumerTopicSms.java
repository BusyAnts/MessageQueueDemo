package com.cimc.consumer05;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 短信消费者
 *
 * @author chenz
 * @create 2019-09-18 19:33
 */
public class ConsumerTopicSms {
    private static final String QUEUE_INFO_SMS = "queue_info_ems";
    private static final String EXCHANGE_TOPIC_INFO = "exchange_topic_info";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        //设置RabbitMQ所在服务器的ip和端口
        factory.setHost("192.168.20.150");
        factory.setPort(5672);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        //声明交换机
        channel.exchangeDeclare(EXCHANGE_TOPIC_INFO, BuiltinExchangeType.TOPIC);
        //声明队列
        channel.queueDeclare(QUEUE_INFO_SMS, true, false, false, null);
        //交换机和队列绑定
        channel.queueBind(QUEUE_INFO_SMS, EXCHANGE_TOPIC_INFO, "info.#.sms.#");
        //定义消费方法
        DefaultConsumer consumer = new DefaultConsumer(channel) {
            /**
             * 消费者接收消息调用此方法
             * @param consumerTag 消费者的标签，在channel.basicConsume()去指定
             * @param envelope 消息包的内容，可从中获取消息id，消息routingKey，交换机，消息和重传标志
            (收到消息失败后是否需要重新发送)
             * @param properties
             * @param body
             * @throws IOException
             */
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                //交换机
                String exchange = envelope.getExchange();
                System.out.println("交换机名称:" + exchange);
                //消息id
                long deliveryTag = envelope.getDeliveryTag();
                System.out.println("消息id:" + deliveryTag);
                //消息内容
                String msg = new String(body, "utf-8");
                System.out.println("receive message.." + msg);
            }
        };
        /**
         * 监听队列String queue, boolean autoAck, Consumer callback
         * 参数明细
         * 1、队列名称
         * 2、是否自动回复，设置为true为表示消息接收到自动向mq回复接收到了，mq接收到回复会删除消息，设置
         为false则需要手动回复
         * 3、消费消息的方法，消费者接收到消息后调用此方法
         */
        channel.basicConsume(QUEUE_INFO_SMS, true, consumer);
    }
}
