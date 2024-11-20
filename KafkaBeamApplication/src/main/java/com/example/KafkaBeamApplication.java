package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDate;
import java.time.Period;

public class KafkaBeamApplication {
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String inputTopic = "input-topic";
        String evenTopic = "EVEN_TOPIC";
        String oddTopic = "ODD_TOPIC";

        Pipeline pipeline = Pipeline.create();

        pipeline.apply("ReadFromKafka", KafkaIO.<String, String>read()
                        .withBootstrapServers(bootstrapServers)
                        .withTopic(inputTopic)
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withoutMetadata())
                .apply("ProcessMessage", ParDo.of(new DoFn<KV<String, String>, KV<String, String>>() {
                    @ProcessElement
                    public void processElement(ProcessContext context) {
                        String message = context.element().getValue();
                        String[] parts = message.split(","); // Assuming CSV payload: "Name,Address,DOB"
                        String name = parts[0];
                        String address = parts[1];
                        String dob = parts[2];

                        int age = calculateAge(dob);
                        String outputMessage = String.format("Name: %s, Address: %s, DOB: %s, Age: %d", name, address, dob, age);

                        if (age % 2 == 0) {
                            context.output(KV.of(evenTopic, outputMessage));
                        } else {
                            context.output(KV.of(oddTopic, outputMessage));
                        }
                    }

                    private int calculateAge(String dob) {
                        LocalDate birthDate = LocalDate.parse(dob);
                        return Period.between(birthDate, LocalDate.now()).getYears();
                    }
                }))
                .apply("WriteToKafka", KafkaIO.<String, String>write()
                        .withBootstrapServers(bootstrapServers)
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer(StringSerializer.class));

        pipeline.run().waitUntilFinish();
    }
}
