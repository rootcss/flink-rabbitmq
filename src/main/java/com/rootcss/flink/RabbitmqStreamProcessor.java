package com.rootcss.flink;

/**
 * Created by rootcss on 13/11/16.
 */

import com.rabbitmq.client.AMQP;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.io.IOException;

public class RabbitmqStreamProcessor extends RMQSource{

    public RabbitmqStreamProcessor(RMQConnectionConfig rmqConnectionConfig, String queueName, DeserializationSchema deserializationSchema) {
        super(rmqConnectionConfig, queueName, deserializationSchema);
    }

    @Override
    protected void setupQueue() throws IOException {
        AMQP.Queue.DeclareOk result = channel.queueDeclare("simpl_spark_dev", true, false, false, null);
        channel.queueBind(result.getQueue(), "simpl_exchange", "*.*.*.*.*");
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Rabbitmq Stream Processor!");
        RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                                                    .setHost("localhost").setPort(5672).setUserName("rootcss")
                                                    .setPassword("indian").setVirtualHost("/").build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = env.addSource(new RMQSource<String>(connectionConfig,
                                                        "simpl_spark_dev",
                                                        new SimpleStringSchema()));

        dataStream.print();

        env.execute();
    }

}