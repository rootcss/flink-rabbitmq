package com.rootcss.flink;

/**
 * Created by rootcss on 13/11/16.
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

public class RabbitmqStreamProcessor extends FlinkRabbitmq {

    public RabbitmqStreamProcessor(RMQConnectionConfig rmqConnectionConfig, String queueName, DeserializationSchema deserializationSchema) {
        super(rmqConnectionConfig, queueName, deserializationSchema);
    }

    public static void main(String[] args) throws Exception {
        logger.info("Starting Rabbitmq Stream Processor..");

        RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(rabbitmqHostname).setPort(rabbitmqPort).setUserName(rabbitmqUsername)
                .setPassword(rabbitmqPassword).setVirtualHost(rabbitmqVirtualHost).build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = env.addSource(new RMQSource<String>(connectionConfig,
                queueName,
                new SimpleStringSchema()));

        DataStream<Tuple2<String, Integer>> pairs = dataStream.flatMap(new TextLengthCalculator());

        pairs.print();
        env.execute("Flink-Rabbitmq Stream Processor");
    }

    public static final class TextLengthCalculator implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            out.collect(new Tuple2<String, Integer>(value, value.length()));
        }

    }

}
