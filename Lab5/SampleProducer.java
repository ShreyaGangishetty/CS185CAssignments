package KafkaExamples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
        
public class SampleProducer {
    // Set the stream and topic to publish to.
    public static String topic;
    // Set the number of messages to send.
    public static int numMessages = 50;
        
    // Declare a new producer.
    public static KafkaProducer<String, String> producer;
        
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        configureProducer();
        topic = args[0];
             // Set content of each message.
        try{
                BufferedReader br = 
                          new BufferedReader(new InputStreamReader(System.in));
                        
                String input;
                        
                while((input=br.readLine())!=null){
                         ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topic, input);
                 producer.send(rec);
                 System.out.println("Sent message  " + input);
                }
                        
        }
        catch(IOException io){
                io.printStackTrace();
        }
        
        
    }
    
           
        
      
    /* Set the value for a configuration parameter.
       This configuration parameter specifies which class
       to use to serialize the value of each message.*/
    public static void configureProducer() {
        Properties props = new Properties();
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("bootstrap.servers", 
                        "localhost:9092");
        props.put("key.serializer", 
                        "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);
    }
}
