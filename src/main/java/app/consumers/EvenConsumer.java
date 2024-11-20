package app.consumers;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class EvenConsumer {

    public static void main(final String[] args) {
        final Properties props = new Properties() {{
            put(BOOTSTRAP_SERVERS_CONFIG, "localhost:50374");
            put(KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getCanonicalName());
            put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
            put(GROUP_ID_CONFIG,                 "kafka-even");
            put(AUTO_OFFSET_RESET_CONFIG,        "earliest");
        }};

        final String evenTopic = "EVEN_TOPIC";
        final String evenMessagesFile = "even-messages.txt";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(evenMessagesFile, true))) {
            try (final Consumer<String, String> evenConsumer = new KafkaConsumer<>(props)) {
                evenConsumer.subscribe(Arrays.asList(evenTopic));
                while (true) {
                    ConsumerRecords<String, String> records = evenConsumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        String key = record.key();
                        String value = record.value();
                        System.out.println(String.format("Consumed event from topic %s: key = [%s]  value = %s", evenTopic, key, value));

                        writer.write(key + " | " + value);
                        writer.newLine();
                    }
                    writer.flush();
                }

            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

}
