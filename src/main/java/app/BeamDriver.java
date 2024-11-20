package app;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.kafka.common.serialization.StringDeserializer;

public class BeamDriver {
    public static void main(String[] args) {

        String inputTopic = "INPUT_TOPIC";
        String evenTopic = "EVEN_TOPIC";
        String oddTopic = "ODD_TOPIC";

        String bootstrapServers = "localhost:50374";

        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        // Read from Kafka
        PCollection<KV<String, String>> inputMessages = pipeline
                .apply("Read from Kafka", KafkaIO.<String, String>read()
                        .withBootstrapServers(bootstrapServers)
                        .withTopic(inputTopic)
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withoutMetadata());

        // Split messages into even and odd
        PCollectionTuple splitMessages = inputMessages.apply("Split Even/Odd", ParDo.of(new DoFn<KV<String, String>, KV<String, String>>() {
            @ProcessElement
            public void processElement(@Element KV<String, String> input, MultiOutputReceiver out) throws Exception {
                System.out.println("Received: " + input);
                String value = input.getValue();
                Person person = new ObjectMapper().readValue(value, Person.class);
                int age = person.getAge();

                if (age % 2 == 0) {
                    //EVEN_TOPIC
                    out.get(EvenOddTag.EVEN_TAG).output(input);
                } else {
                    //OUT_TOPIC
                    out.get(EvenOddTag.ODD_TAG).output(input);
                }
            }
        }).withOutputTags(EvenOddTag.EVEN_TAG, TupleTagList.of(EvenOddTag.ODD_TAG)));

        PCollection<KV<String, String>> evenMessages = splitMessages.get(EvenOddTag.EVEN_TAG)
                .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

        PCollection<KV<String, String>> oddMessages = splitMessages.get(EvenOddTag.ODD_TAG)
                .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

        // Write even messages to Kafka
        evenMessages.apply("Write Even Messages", ParDo.of(new KafkaWriteTransform(evenTopic, bootstrapServers)));

        // Write odd messages to Kafka
        oddMessages.apply("Write Odd Messages", ParDo.of(new KafkaWriteTransform(oddTopic, bootstrapServers)));

        // Run the pipeline
        pipeline.run().waitUntilFinish();
    }
}