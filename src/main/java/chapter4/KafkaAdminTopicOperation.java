package chapter4;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;

/**
 * 代码清单4-3 & 4-4
 * Created by 朱小厮 on 2018/7/21.
 */
public class KafkaAdminTopicOperation {
    public static final String brokerList = "localhost:9092";
    public static final String topic = "topic-admin";

    public static void describeTopic(){
        String brokerList =  "localhost:9092";
        String topic = "topic-admin";

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient client = AdminClient.create(props);

        DescribeTopicsResult result = client.describeTopics(Collections.singleton(topic));
        try {
            Map<String, TopicDescription> descriptionMap =  result.all().get();
            System.out.println(descriptionMap.get(topic));
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }

    public static void createTopic() {
        String brokerList = "localhost:9092";
        // 主题名称
        String topic = "topic-admin";

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        // The configuration controls the maximum amount of time the client will wait for the response of a request.
        // If the response is not received before the timeout elapses the client will resend the request if necessary
        // or fail the request if retries are exhausted.

        /**
         * REQUEST_TIMEOUT_MS_CONFIG = "request.timeout.ms"
         * 这个配置在创建 adminClient 时，被传递进去；是指 client 等待请求响应的时间
         * 如果响应在这个超时时间内不能得到，client 则会重试，重试到一定次数则会失败；
         * 以下是因为介绍：
         * The configuration controls the maximum amount of time the client will wait for the response of a request.
         * If the response is not received before the timeout elapses the client will resend the request if necessary
         * or fail the request if retries are exhausted.
         */
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient client = AdminClient.create(props);

        //        NewTopic newTopic = new NewTopic(topic, 4, (short) 1);

        //        Map<String, String> configs = new HashMap<>();
        //        configs.put("cleanup.policy", "compact");
        //        newTopic.configs(configs);

        Map<Integer, List<Integer>> replicasAssignments = new HashMap<>();
        replicasAssignments.put(0, Arrays.asList(0));
        replicasAssignments.put(1, Arrays.asList(0));
        replicasAssignments.put(2, Arrays.asList(0));
        replicasAssignments.put(3, Arrays.asList(0));

        NewTopic newTopic = new NewTopic(topic, replicasAssignments);

        //代码清单4-4 可以从这里跟进去
        CreateTopicsResult result = client.
                createTopics(Collections.singleton(newTopic));
        try {
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }

    public static void deleteTopic(){
        String brokerList =  "localhost:9092";
        String topic = "topic-admin";

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient client = AdminClient.create(props);

        try {
            client.deleteTopics(Collections.singleton(topic)).all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        createTopic();
        //        describeTopic();
        //        deleteTopic();

        //        Properties props = new Properties();
        //        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        //        props.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        //        AdminClient client = AdminClient.create(props);
        ////        createTopic(client);
        ////        deleteTopic(client);
        //
        //        NewTopic newTopic = new NewTopic(topic, 4, (short) 3);
        //        Map<String, String> configs = new HashMap<>();
        //        configs.put("cleanup.policy", "compact");
        //        newTopic.configs(configs);
        //        createTopic(client,newTopic);
    }

//    public static void deleteTopic(AdminClient client) throws ExecutionException, InterruptedException {
//        DeleteTopicsResult result = client.deleteTopics(Arrays.asList(topic));
//        result.all().get();
//    }
//
//    public static void createTopic(AdminClient client, NewTopic newTopic) throws ExecutionException,
//            InterruptedException {
//        CreateTopicsResult result = client.createTopics(Collections.singleton(newTopic));
//        result.all().get();
//    }
//
//    public static void createTopic(AdminClient client) throws ExecutionException, InterruptedException {
//        //创建一个主题：topic-demo，其中分区数为4，副本数为1
//        NewTopic newTopic = new NewTopic(topic, 4, (short) 1);
//        CreateTopicsResult result = client.createTopics(Collections.singleton(newTopic));
//        result.all().get();
//    }
}
