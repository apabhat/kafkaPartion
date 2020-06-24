

import org.apache.kafka.clients.producer.KafkaProducer;

import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


/**
 * Created by sunilpatil on 12/25/16.
 */
public class Producer {
	
    public static void main(String[] argv)throws Exception {
    	KafkaProducer<String, String> producer;
    	String topic;
    	

    	final String KAFKA_SERVER_URL = "localhost";
    	final int KAFKA_SERVER_PORT = 9092;
    	final String CLIENT_ID = "SampleProducer";

    	
    		Properties properties = new Properties();
    		properties.put("bootstrap.servers", KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
    		properties.put("client.id", CLIENT_ID);
    		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//    		properties.put("retries",0);
//    		properties.put("request.timeout.ms", 5000);
    		properties.put("partitioner.class", "customPartitioner");

    		properties.put("message.type.name", "hi");

    		producer = new KafkaProducer<String, String>(properties);
    		
    		
    		topic = "test2";
//    	int messageNo=10;
    	String messageStr = "Aparna";
// 


    	
    	try {
    		RecordMetadata metadata = null;
    		
    		
    		for (int i = 0; i < 10; i++) {
    			
    			Thread.sleep(2000);
    			metadata = producer.send(new ProducerRecord<>(topic, "hi", messageStr+i)).get();
    		//	System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");

    			System.out.println("Message is sent to Partition no " + metadata.partition() + " and offset "
    					+ metadata.offset());
    			System.out.println("SynchronousProducer Completed with success.");

    		}

            for (int i = 0; i < 10; i++) {
            	Thread.sleep(2000);
                metadata=producer.send(new ProducerRecord<>(topic, "hello"+i, messageStr+i)).get();
         //       System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");

    			System.out.println("Message is sent to Partition no " + metadata.partition() + " and offset "
    					+ metadata.offset());
    			System.out.println("SynchronousProducer Completed with success.");

            }
    		
//			RecordMetadata metadata = (RecordMetadata) producer
//					.send(new ProducerRecord<Integer, String>(topic, messageNo, messageStr+String.valueOf(messageNo))).get();
			
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			System.out.print("Caught exception in synchronous producer");
			// handle the exception
		}
    	
    	
    	
       producer.close();
    }
}