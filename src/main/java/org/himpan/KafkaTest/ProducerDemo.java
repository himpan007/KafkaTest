package org.himpan.KafkaTest;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

	public static void main(String[] args) {
		String bootstrapServers = "127.0.0.1:9092";
		
		//create producer properties
		
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers );
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
    // create a prodcuer
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
				
	
	//Create producer record
	

	  ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello Franz Kafka");
	
	//send the data- Asynchronous
	producer.send(record, new Callback() {
		
		public void onCompletion(RecordMetadata metadata, Exception exception) {
			// TODO Auto-generated method stub
		}
	});
	producer.flush();
	producer.close();
	
	}
}
