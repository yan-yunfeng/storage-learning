package curd;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import connector.Connectors;

/**
 * author Yan YunFeng  Email:twd.wuyun@163.com
 * create 19-8-9 下午3:01
 */
public class Kafka {
    public static void main(String[] args) {
        //生产者
        KafkaProducer<String, String> kafkaProducer = Connectors.getKafkaProducer();
        //这里设置key，可以保证相同的key被分到kafka的同一个分区
        kafkaProducer.send(new ProducerRecord<>("topic", "key", "value"));

        //消费者
        KafkaConsumer<String, String> kafkaConsumer = Connectors.getKafkaConsumer("topic", "groupId");
        while (true) {
            kafkaConsumer.poll(100)
                         .iterator()
                         .forEachRemaining(record -> {
                             String content = record.value();
                             System.out.println(content);
                         });
        }
    }
}
