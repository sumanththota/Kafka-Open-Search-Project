package org.example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log  = LoggerFactory.getLogger(ProducerDemo.class);
    public static void main(String[] args) {

        // create Producer Properties
        Properties properties = new Properties();


        //connect to local host
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        //properties.setProperty("batch.size", "30");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
            for( int i =0; i < 30; i++){

                String topic = "demo_java";
                String key = "ID"+i;
                String value = "hello world"+i;
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
                // this sends to the producer buffer without waiting to send to the broker
                //asynchronous
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e == null){
                            log.info("key:" + key + "|" + "partition:" + recordMetadata.partition());
                        }else{
                            log.error("Error while producing", e);
                        }
                    }
                });
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }


        }




        // this sends to the everything into the broker and synchronously and waits untill
        // all the messages are sent to the broker
        producer.flush();
        producer.close();

    }
}