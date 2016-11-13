Execute using (Make sure flink & rabbitmq is up and running):
```bash
flink run -c com.rootcss.flink.RabbitmqStreamProcessor target/flink-rabbitmq-0.1.jar
```
<br>
Build using:
```bash
mvn clean package
```
