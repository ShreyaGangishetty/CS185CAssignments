package auditlog;

import java.io.File;
import java.util.Properties;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Log2Topic extends TailerListenerAdapter{

	private static String auditTopic;
	private static String auditFilePath;
	private static String brokerName;
	private static String brokerPort;
	private static KafkaProducer<String, String> auditLogProducer;

	public void handle(String line) {
		System.out.println(line);
		//Writing data to auditTopic
		// Set content of each Kafka producerRecord .
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(auditTopic, line);
		auditLogProducer.send(producerRecord);
		//System.out.println("Sent message  " + line);
	}
	
	public static void main(String[] args) throws InterruptedException {
		/*
		 * 
java -cp CS185-jar-with-dependencies.jar auditlog.Log2Topic /var/log/audit/audit.log auditTopic brokerName brokerPort

args[0] ==> /var/log/audit/audit.log
args[1]==> auditTopic
args[2] ==> brokerName 
args[3] ==> brokerPort

*/
		auditFilePath=args[0];
		auditTopic=args[1];
		brokerName=args[2];
		brokerPort=args[3];
		Properties producerProperties= new Properties();
		producerProperties.put("metadata.broker.list", brokerName+":"+brokerPort);
		producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producerProperties.put("bootstrap.servers", brokerName+":"+brokerPort);
		auditLogProducer = new KafkaProducer<String, String>(producerProperties);//configured producer
		
		TailerListenerAdapter listener = new Log2Topic();		
		Tailer tail = new Tailer(new File(auditFilePath),listener,1,true);
		tail.run();
		tail.stop();

	}
}