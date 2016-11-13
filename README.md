Execute using (Make sure flink & rabbitmq is up and running):
```bash
flink run -c com.rootcss.flink.RabbitmqStreamProcessor target/flink-rabbitmq-0.1.jar
```

Build using:
```bash
mvn clean package
```

Open Dashboard:
```bash
http://localhost:8081/
```
![alt tag](doc/images/flink_dashboard.png)