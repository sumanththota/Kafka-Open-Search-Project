package sumanthdev.kafka.wikimedia;


import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements BackgroundEventHandler {
    KafkaProducer<String, String> producer;
    String topic;
    private final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    public WikimediaChangeHandler(KafkaProducer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {
        //nothing to do

    }

    @Override
    public void onClosed() {
        producer.close();

    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        log.info( messageEvent.getData());
        producer.send(new ProducerRecord<>(topic, messageEvent.getData()));

    }

    @Override
    public void onComment(String s) throws Exception {
        //nothing to do
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Error in EventSource", throwable);

    }
}
