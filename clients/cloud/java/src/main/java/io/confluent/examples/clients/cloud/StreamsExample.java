package io.confluent.examples.clients.cloud;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

import io.confluent.examples.clients.cloud.model.DataRecord;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;

public class StreamsExample {

    public static void main(String[] args) throws Exception {

        final String topic = "foo";
    
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "demo-streams-1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Serde<DataRecord> DataRecord = getJsonSerde();

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, DataRecord> records = builder.stream(topic, Consumed.with(Serdes.String(), DataRecord));

        KStream<String,Long> counts = records.map((k, v) -> new KeyValue<String, Long>(k, v.getCount()));
        counts.print(Printed.<String,Long>toSysOut().withLabel("Consumed record"));

        // Aggregate values by key
        KStream<String,Long> countAgg = counts.groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
            .reduce(
                (aggValue, newValue) -> aggValue + newValue)
            .toStream();
        countAgg.print(Printed.<String,Long>toSysOut().withLabel("Running count"));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    private static Serde<DataRecord> getJsonSerde(){

        Map<String, Object> serdeProps = new HashMap<>();
        serdeProps.put("json.value.type", DataRecord.class);

        final Serializer<DataRecord> mySerializer = new KafkaJsonSerializer<>();
        mySerializer.configure(serdeProps, false);

        final Deserializer<DataRecord> myDeserializer = new KafkaJsonDeserializer<>();
        myDeserializer.configure(serdeProps, false);

        return Serdes.serdeFrom(mySerializer, myDeserializer);
    }
}
