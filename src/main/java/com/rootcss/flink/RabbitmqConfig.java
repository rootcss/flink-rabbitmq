package com.rootcss.flink;

/**
 * Created by rootcss on 11/12/16.
 */
public class RabbitmqConfig {
    public static String exchangeName          = "simpl_exchange";
    public static String queueName             = "simpl_spark_dev";
    public static String rabbitmqHostname      = "localhost";
    public static String rabbitmqVirtualHost   = "/";
    public static String rabbitmqUsername      = "rootcss";
    public static String rabbitmqPassword      = "indian";
    public static Integer rabbitmqPort         = 5672;
    public static boolean durableQueue         = false;
}
