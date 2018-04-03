package auditlog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka09.ConsumerStrategies;
import org.apache.spark.streaming.kafka09.KafkaUtils;
import org.apache.spark.streaming.kafka09.LocationStrategies;
import org.apache.spark.streaming.Duration;
import com.google.common.collect.Lists;
import scala.Tuple2;

public class LoginAnomalyDetector {

	private static String auditTopic;
	private static String anomalyTopic;
	private static String broker;
	private static Integer windowSize;
	private static Integer noOfLoginAttemptsFailed;
	private static Long sparkBatchInterval;
	private static KafkaProducer<String, String> anomalyTopicProducer;
	private static String masterURL="local[*]";
	private static Duration windowDuration; 
	private static final Pattern SPACE = Pattern.compile("[\\s+\\t]");

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		/* /opt/mapr/spark/spark-2.1.0/bin/spark-submit --master local[*]
		 *  --class auditlog.LoginAnomalyDetector
		 *  0) localhost:9092 ==> <broker> 
		 *  1) auditRecords ==> <in-topic>
		 *  2) loginAnomalies ==>  <out-topic>
		 *  3) 5 ==>  <number-of-failed-login-attempts>
		 *  4) 5000 ==>  <interval-in-ms>
		 *  5) 30000 ==>  <window-size-in-ms>
		 *  Usage: auditlog.LoginAnomalyDetector <broker> <in-topic> <out-topic> <number-of-failed-login-attempts> <interval-in-ms> <window-size-in-ms>

		 */
		broker=args[0];
		auditTopic=args[1];
		anomalyTopic=args[2];
		noOfLoginAttemptsFailed=Integer.parseInt(args[3]);
		sparkBatchInterval=Long.parseLong(args[4]);
		windowSize=Integer.parseInt(args[5]);
		windowDuration=new Duration(windowSize);
		// check command-line args
		if(args.length != 6) {
			System.err.println("usage: LoginAnomalyDetector <port-number> <master-url>..");
			System.exit(1);
		}

		// turn off logging
		LogManager.getRootLogger().setLevel(Level.OFF);     

		// Create a StreamingContext 
		JavaStreamingContext jssc = new JavaStreamingContext(masterURL, "LoginAnomalyDetector", 
				new Duration(sparkBatchInterval));

		//kafka dstream
		List<String> topics = new ArrayList<String>();
		topics.add(auditTopic);
		Map<String, Object> topicProperties = new HashMap<String, Object>();
		topicProperties.put("metadata.broker.list", broker);
		topicProperties.put("group.id", "consumer-gpid");
		topicProperties.put("bootstrap.servers", broker);
		topicProperties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");  
		topicProperties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");	
		topicProperties.put("auto.offset.reset", "latest");
		topicProperties.put("enable.auto.commit",false);

		JavaInputDStream<ConsumerRecord<Object, Object>> messages=
				KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
						ConsumerStrategies.Subscribe(topics, topicProperties));

		JavaDStream<String> lines=messages.transform(
				new Function<JavaRDD<ConsumerRecord<Object,Object>>, JavaRDD<String>>()  
				{
					private static final long serialVersionUID = 1L;
					public JavaRDD<String> call(JavaRDD<ConsumerRecord<Object, Object>> line)
							throws Exception {
						// TODO Auto-generated method stub
						JavaRDD<String> transformedLines=line.flatMap(
								new FlatMapFunction<ConsumerRecord<Object,Object>, String>() {
									private static final long serialVersionUID = 1L;
									public Iterator<String> call(ConsumerRecord<Object, Object> line) throws Exception {
										// TODO Auto-generated method stub
										return Lists.newArrayList(line.value().toString()).iterator();
									}
								});
						return transformedLines;
					}
				});
		//lines.print();

		JavaDStream<String> failedUserIDs = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;
			public Iterator<String> call(String record) {
				List<String> failedUID = new ArrayList<String>();
				if(record.contains("USER_LOGIN") && record.contains("failed") && record.contains("acct"))
				{
					String [] splitArray = SPACE.split(record);
					for (String string : splitArray) {
						if(string.contains("acct"))
						{
						String acct[] = string.split("=");
						failedUID.add(acct[1]);
						System.out.println("acct is: "+acct[1]);
						break;
						}
					}
				}				
				return failedUID.iterator();
			}
		});

		//failedUserIDs.print();

		JavaPairDStream<String, Integer> anomalousUID = failedUserIDs.mapToPair(
				new PairFunction<String, String, Integer>() { 
					private static final long serialVersionUID = 1L;
					public Tuple2<String, Integer> call(String s) { 
						return new Tuple2<String, Integer>(s, 1); 
					}
				}).reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() { 
					private static final long serialVersionUID = 1L;
					public Integer call(Integer i1, Integer i2) { 
						return i1 + i2; 
					}}, windowDuration);

		anomalousUID.print();

		Properties producerProperties = new Properties();
		producerProperties.put("metadata.broker.list", broker);
		producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producerProperties.put("bootstrap.servers", broker);
		producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		anomalyTopicProducer = new KafkaProducer<String, String>(producerProperties);

		anomalousUID.foreachRDD(new VoidFunction<JavaPairRDD<String,Integer>>(){
			private static final long serialVersionUID = 1L;
			public void call(JavaPairRDD<String, Integer> arg0) throws Exception {
				// TODO Auto-generated method stub
				Map<String,Integer> keyValue=arg0.collectAsMap();
				for(String k:keyValue.keySet())
				{
					//System.out.println("in for: writeanomtopic");
					if(k.length()!=0 && keyValue.get(k)>=noOfLoginAttemptsFailed) 
					{
						//System.out.println("inside write to anomaly topic");
						ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(anomalyTopic, "user accountname= "+k+", number of failed login attempts="+keyValue.get(k));
						anomalyTopicProducer.send(producerRecord);
						//System.out.println("\nSent message to anomalytopic" + k+","+keyValue.get(k));
					}
				}
			}			
		});

		// start streaming context
		jssc.start();
		try {
			// run until terminated (killed)
			jssc.awaitTermination();
		}
		catch(Exception e) {
			System.err.println(e.getLocalizedMessage());
		}
		// close spark context
		jssc.close();
	}
}