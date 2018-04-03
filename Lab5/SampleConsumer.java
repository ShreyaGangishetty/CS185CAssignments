package KafkaExamples;

import org.apache.kafka.clients.consumer.*;
import java.io.IOException;
import java.util.*;

public class SampleConsumer {
    // Set the stream and topic to read from.
    public static String topic;

    // Declare a new consumer.
    public static KafkaConsumer<String, String> consumer;

    public static void main(String[] args) throws IOException {
        configureConsumer();
        topic=args[0];

        // Subscribe to the topic.
        List<String> topics = new ArrayList<String>();
        topics.add(topic);
        consumer.subscribe(topics);

        // Set the timeout interval for requests for unread messages.
        long pollTimeOut = 1000;
        while(true) {
            // Request unread messages from the topic.
            ConsumerRecords<String, String> consumerRecords = consumer.poll(pollTimeOut);
            Iterator<ConsumerRecord<String, String>> iterator = consumerRecords.iterator();
            while (iterator.hasNext()) {
                    ConsumerRecord<String, String> record = iterator.next();
                    // Iterate through returned records, extract the value
                    // of each message, and print the value to standard output.
                    System.out.println((" Consumed Record: " + record.toString()));
             } 
            
        }
      
    }

    /* Set the value for a configuration parameter.
       This configuration parameter specifies which class
       to use to deserialize the value of each message.*/
    public static void configureConsumer() {
        Properties props = new Properties();
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("bootstrap.servers", 
                        "localhost:9092");
        props.put("key.deserializer", 
                        "org.apache.kafka.common.serialization.StringDeserializer");


        consumer = new KafkaConsumer<String, String>(props);
    }
}
