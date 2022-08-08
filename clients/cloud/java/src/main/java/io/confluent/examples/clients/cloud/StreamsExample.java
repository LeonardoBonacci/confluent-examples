package io.confluent.examples.clients.cloud;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import io.confluent.examples.clients.cloud.model.Aggregation;
import io.confluent.examples.clients.cloud.model.DataRecord;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;

public class StreamsExample {

    public static void main(String[] args) throws Exception {

        final String topic = "foo";
    
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "demo-streams");

        final Serde<DataRecord> DataRecord = getJsonSerde();

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, DataRecord> records = builder.stream(topic, Consumed.with(Serdes.String(), DataRecord));

        KStream<String,Long> counts = records
            .map((k, v) -> new KeyValue<String, Long>(k, v.getCount()));
        counts.print(Printed.<String,Long>toSysOut().withLabel("Consumed record"));
//        counts.to("bar", Produced.with(Serdes.String(), Serdes.Long()));

//        KStream<String,Long> countAgg = counts
//          .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
//          .count()
//          .toStream();
//        countAgg.print(Printed.<String, Long>toSysOut().withLabel("Running count"));
        
//        KStream<String,Long> countAgg = counts
//            .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
//            .reduce(
//                (aggValue, newValue) -> aggValue + newValue)
//            .toStream();
//        countAgg.print(Printed.<String, Long>toSysOut().withLabel("Running count"));

//        KStream<Windowed<String>,Long> countAgg = counts
//            .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
//            .windowedBy(TimeWindows
//                            .ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(2)))
//            .reduce(
//                (aggValue, newValue) -> aggValue + newValue)
//            .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
//            .toStream();
//
//        countAgg.print(Printed.<Windowed<String>,Long>toSysOut().withLabel("Running count"));

        KStream<Windowed<String>,Aggregation> countAgg = counts
            .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
            .windowedBy(TimeWindows
                            .ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(2)))
            .aggregate(                                                   
                Aggregation::new,
                (key, value, aggregation) -> aggregation.updateFrom(value),
                Materialized.with(Serdes.String(), getAggregationSerde())
            )
            .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
            .toStream();

        countAgg.print(Printed.<Windowed<String>,Aggregation>toSysOut().withLabel("Running count"));

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
    
    private static Serde<Aggregation> getAggregationSerde(){

      Map<String, Object> serdeProps = new HashMap<>();
      serdeProps.put("json.value.type", Aggregation.class);

      final Serializer<Aggregation> mySerializer = new KafkaJsonSerializer<>();
      mySerializer.configure(serdeProps, false);

      final Deserializer<Aggregation> myDeserializer = new KafkaJsonDeserializer<>();
      myDeserializer.configure(serdeProps, false);

      return Serdes.serdeFrom(mySerializer, myDeserializer);
  }

}
