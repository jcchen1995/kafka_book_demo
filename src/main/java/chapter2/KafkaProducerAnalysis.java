package chapter2;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * 代码清单2-1
 * Created by 朱小厮 on 2018/8/29.
 */
public class KafkaProducerAnalysis {
    public static final String brokerList = "localhost:9092";
    public static final String topic = "topic-demo";

    public static Properties initConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("client.id", "producer.client.id.demo");
        return props;
    }

    public static Properties initNewConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        return props;
    }

    public static Properties initPerferConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        return props;
    }

    public static void main(String[] args) throws InterruptedException {
        Properties props = initConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

//        KafkaProducer<String, String> producer = new KafkaProducer<>(props,
//                new StringSerializer(), new StringSerializer());

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "hello, Kafka!");
        try {
            // 1.「发后即忘」模式；以为发送后，不会对返回值 Future<RecordMetadata> 进行判定
            producer.send(record);
            // 2.「同步」模式
            //            producer.send(record).get();

            // 3-1.「异步」模式
            //            Future<RecordMetadata> future = producer.send(record);
            //            // do something...
            //            RecordMetadata recordMetadata = future.get();

            // 3-2.「异步」模式，增加回调方法
            //            Future<RecordMetadata> future = producer.send(record, (metadata, exception) -> {
            //                // do something..
            //            });

            //            producer.send(record, new Callback() {
            //                @Override
            //                public void onCompletion(RecordMetadata metadata, Exception exception) {
            //                    if (exception == null) {
            //                        System.out.println(metadata.partition() + ":" + metadata.offset());
            //                    }
            //                }
            //            });
        } catch (Exception e) {
            e.printStackTrace();
        }

//        TimeUnit.SECONDS.sleep(5);
    }
}
