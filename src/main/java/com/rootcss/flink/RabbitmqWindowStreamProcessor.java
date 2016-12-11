package com.rootcss.flink;

/**
 * Created by rootcss on 13/11/16.
 */

import com.rabbitmq.client.AMQP;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class RabbitmqWindowStreamProcessor extends RMQSource {

    static String exchangeName          = "simpl_exchange";
    static String queueName             = "simpl_spark_dev";
    static String rabbitmqHostname      = "localhost";
    static String rabbitmqVirtualHost   = "/";
    static String rabbitmqUsername      = "rootcss";
    static String rabbitmqPassword      = "indian";
    static boolean durableQueue         = false;
    static Integer rabbitmqPort         = 5672;
    static int windowSeconds            = 10;
    static String outputFile            = "/Users/rootcss/infrastructure/codes/flink/flink-rabbitmq/output.out";

    private static Logger logger = LoggerFactory.getLogger(RabbitmqStreamProcessor.class);

    public RabbitmqWindowStreamProcessor(RMQConnectionConfig rmqConnectionConfig, String queueName, DeserializationSchema deserializationSchema) {
        super(rmqConnectionConfig, queueName, deserializationSchema);
    }

    @Override
    protected void setupQueue() throws IOException {
        AMQP.Queue.DeclareOk result = channel.queueDeclare(queueName, true, durableQueue, false, null);
        channel.queueBind(result.getQueue(), exchangeName, "*");
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

        DataStream<Tuple2<String, Integer>> pairs = dataStream
                .flatMap(new TextLengthCalculator())
                .keyBy(0)
                .timeWindow(Time.seconds(windowSeconds))
                .sum(1);

        pairs.writeAsText(outputFile);
        env.execute();
    }

    public static final class TextLengthCalculator implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            out.collect(new Tuple2<String, Integer>(value, value.length()));
        }

    }

}
