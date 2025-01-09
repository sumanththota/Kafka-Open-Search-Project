package sumanthdev.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.StreamException;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangeProducer {
    public static void main(String[] args) throws InterruptedException {

        String bootstrapServers = "127.0.0.1:9092";
        final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String topic = "wikimedia.recentchange";

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        BackgroundEventHandler eventHandler = new WikimediaChangeHandler(producer,topic);
        EventSource.Builder eventSourceBuilder = new EventSource.Builder(URI.create(url));
        BackgroundEventSource.Builder builder = new BackgroundEventSource.Builder(eventHandler,eventSourceBuilder);
        BackgroundEventSource eventSource = builder.build();


//start the producer in another thread
        eventSource.start();
        log.info("Event source  started");

        TimeUnit.MINUTES.sleep(10);

    }

}
