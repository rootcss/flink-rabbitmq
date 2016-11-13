package com.rootcss.flink;

/**
 * Created by rootcss on 13/11/16.
 */

import com.rabbitmq.client.AMQP;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RabbitmqStreamProcessor extends RMQSource{

    private static Logger logger = LoggerFactory.getLogger(RabbitmqStreamProcessor.class);

    public RabbitmqStreamProcessor(RMQConnectionConfig rmqConnectionConfig, String queueName, DeserializationSchema deserializationSchema) {
        super(rmqConnectionConfig, queueName, deserializationSchema);
    }

    @Override
    protected void setupQueue() throws IOException {
        AMQP.Queue.DeclareOk result = channel.queueDeclare("simpl_spark_dev", true, false, false, null);
        channel.queueBind(result.getQueue(), "simpl_exchange", "*.*.*.*.*");
    }

    public static void main(String[] args) throws Exception {
        logger.info("Starting Rabbitmq Stream Processor..");

        RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost("localhost").setPort(5672).setUserName("rootcss")
                .setPassword("indian").setVirtualHost("/").build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = env.addSource(new RMQSource<String>(connectionConfig,
                "simpl_spark_dev",
                new SimpleStringSchema()));

        DataStream<Tuple2<String, Integer>> pairs = dataStream.flatMap(new TextLengthCalculator());

        pairs.print();
        env.execute();
    }

    public static final class TextLengthCalculator implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            out.collect(new Tuple2<String, Integer>(value, value.length()));
        }

    }
}
