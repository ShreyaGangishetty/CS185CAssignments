package SparkExamples;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;

import com.google.common.collect.Lists;

import scala.Tuple2;

import java.util.Iterator;
import java.util.regex.Pattern;
/**
 * 
 * 
 * 1. run nc command to generate data in a tcp port
 * nc -lk 9999
 * 2. launch this application
 * /opt/mapr/spark/spark-2.0.1/bin/spark-submit \
 * --class SparkExamples.StreamingWordCount \
 * CS185.jar  \
 * 9999 \
 * local[2]
 */


public class StreamingWordCount {
          private static final Pattern SPACE = Pattern.compile("[\\s+\\t]");

        public static void main(String[] args) {
            // check command-line args
            if(args.length != 2) {
                System.err.println("usage: StreamingWordCount <port-number> <master-url>");
                System.exit(1);
            }
                  
                 
            // turn off logging
            LogManager.getRootLogger().setLevel(Level.OFF);     
            
            // set tcp port to listen to
            int port = Integer.parseInt(args[0]);
            
            // set deployment mode
            String masterURL = args[1];
            
            // Create a StreamingContext 
            JavaStreamingContext jssc = new JavaStreamingContext(masterURL, "StreamingWordCount", 
                new Duration(5000));
            
            // Create a DStream that will connect to 127.0.0.1:port socket
            JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", port);
            
            // print lines read from socket
            lines.print();
            
            // tokenize lines into individual words
            JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
                private static final long serialVersionUID = 1L;
                public Iterator<String> call(String x) {
                        return Lists.newArrayList(SPACE.split(x)).iterator();
                }
            });
            
            // print words from lines
            words.print();
            
            // calculate word counts from words
            JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                new PairFunction<String, String, Integer>() { 
                    private static final long serialVersionUID = 1L;
                    public Tuple2<String, Integer> call(String s) { 
                        return new Tuple2<String, Integer>(s, 1); 
                    }
                })
             .reduceByKey(new Function2<Integer, Integer, Integer>() { 
                 private static final long serialVersionUID = 1L;
                 public Integer call(Integer i1, Integer i2) { 
                     return i1 + i2; 
                 }
             });
        
            // print word counts
            wordCounts.print();
            
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

