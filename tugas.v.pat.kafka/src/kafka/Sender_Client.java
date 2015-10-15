/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package kafka;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import static scala.App$class.args;
/**
 *
 * @author adwisatya
 */
public class Sender_Client {
    public void send_message(){
        long events = Long.parseLong("123234");
        Random rnd = new Random();
 
        Properties props = new Properties();
        props.put("metadata.broker.list", "broker1:9092,broker2:9092 ");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "example.producer.SimplePartitioner");
        props.put("request.required.acks", "1");
 
        ProducerConfig config = new ProducerConfig(props);
 
        Producer<String, String> producer = new Producer<String, String>(config);
 
        KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", "127.0.0.1", "hello");
        producer.send(data);
        producer.close();
    }
}
