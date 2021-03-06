package com.rootcss.flink;

/**
 * Created by rootcss on 13/11/16.
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

public class RabbitmqMessageRateCalculator extends FlinkRabbitmq {

    static int windowSeconds = 1;

    public RabbitmqMessageRateCalculator(RMQConnectionConfig rmqConnectionConfig, String queueName, DeserializationSchema deserializationSchema) {
        super(rmqConnectionConfig, queueName, deserializationSchema);
    }

    public static void main(String[] args) throws Exception {
        logger.info("Starting Rabbitmq Stream Processor..");

        final ParameterTool params = ParameterTool.fromArgs(args);

        RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(rabbitmqHostname).setPort(rabbitmqPort).setUserName(rabbitmqUsername)
                .setPassword(rabbitmqPassword).setVirtualHost(rabbitmqVirtualHost).build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = env.addSource(new RMQSource<String>(connectionConfig,
                queueName,
                new SimpleStringSchema()));

        DataStream<Tuple2<String, Integer>> pairs = dataStream
                .flatMap(new TextLengthCalculator())
                .keyBy(0)
                .timeWindow(Time.seconds(windowSeconds))
                .sum(1);

        if (params.has("output")) {
            pairs.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            pairs.print();
        }
        env.execute("Flink-Rabbitmq Stream Processor");
    }

    public static final class TextLengthCalculator implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            out.collect(new Tuple2<String, Integer>("message/sec", 1));
        }

    }

}