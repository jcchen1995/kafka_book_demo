package chapter1;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * 代码清单1-1
 * Created by 朱小厮 on 2018/7/21.
 */
public class ProducerFastStart {
    public static final String brokerList = "localhost:9092";
    public static final String topic = "topic-demo";

    public static void main(String[] args) {
        Properties properties = new Properties();
        // 关键字的序列化类，实现以下接口： org.apache.kafka.common.serialization.Serializer 接口。
        // todo jc 这里的关键字指的是什么？
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        // 值的序列化类，实现以下接口： org.apache.kafka.common.serialization.Serializer 接口。
        // todo jc 这里的值指的是什么？
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        // 连接的 kafka broker 的地址，如果是集群，填写其中一个即可
        properties.put("bootstrap.servers", brokerList);


        KafkaProducer<String, String> producer =
                new KafkaProducer<>(properties);
        StringBuilder recordValueBuilder = new StringBuilder("hello, Kafka!");
        for (int i = 0; i < 50; i++) {
            recordValueBuilder.append(" hello, Kafka!");
        }
        System.err.println(recordValueBuilder.toString());
        while (true) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, recordValueBuilder.toString());
            try {
                producer.send(record);
                //            producer.send(record).get();
            } catch (Exception e) {
                e.printStackTrace();
            }
            //            try {
            //                Thread.sleep(3000);
            //            } catch (InterruptedException e) {
            //                e.printStackTrace();
            //            }
        }
        //        producer.close();
    }
}
