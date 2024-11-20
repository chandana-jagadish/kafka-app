package app;

import app.producer.InputProducer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class KafkaWriteTransform extends DoFn<KV<String, String>, Void> {
    private final String topic;
    private final String bootstrapServers;
    private transient InputProducer inputProducer;

    public KafkaWriteTransform(String topic, String bootstrapServers) {
        this.topic = topic;
        this.bootstrapServers = bootstrapServers;
    }

    @Setup
    public void setup() {
        inputProducer = new InputProducer(bootstrapServers);
    }

    @ProcessElement
    public void processElement(@Element KV<String, String> element) {
        inputProducer.sendMessage(topic, element.getKey(), element.getValue());
    }

    @Teardown
    public void teardown() {
        inputProducer.close();
    }
}

