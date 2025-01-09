package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {


        //consumer group
        String groupId = "my-Kafka-Application";
        String Topic = "demo_java";
        // create Producer Properties
        Properties properties = new Properties();
        //connect to local host
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        // set consumer properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        //get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

//         adding a shut down hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("Detected a shut down, calling consumer.wakeup()");

                consumer.wakeup();
                log.info("After calling wakeup");


                try{
                    log.info("before joinng main thread");
                    mainThread.join();
                    log.info("after joinng main thread");

                }
                catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        });
        consumer.subscribe(Arrays.asList(Topic));
        try{
            while(true){
                // poll for new data
                log.info("polling");
                ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(3000));
                for(ConsumerRecord<String, String> record : records){
                    log.info("Key: " + record.key() + " Value: " + record.value());
                    log.info("Partition: " + record.partition() + " Offset: " + record.offset());
                }


            }
        }
        catch(WakeupException e){
            log.info("Received shutdown signal");
        }
        catch (Exception e){
            log.error("Error in consumer", e);
        }
        finally {
            consumer.close();
        }



    }
}
