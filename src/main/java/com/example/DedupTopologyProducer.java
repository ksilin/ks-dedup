package com.example;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.jboss.logging.Logger;

import java.util.Properties;

@ApplicationScoped
public class DedupTopologyProducer {

    static Logger log = Logger.getLogger(DedupTopologyProducer.class);

    @Produces
    Topology makeTopology(){

        return createTopology();
    }

    static public Topology createTopology(){

        String storeName = "deduplication-store";


        StreamsBuilder builder = new StreamsBuilder();

        StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(storeName),
                Serdes.String(),
                Serdes.String());
        builder.addStateStore(storeBuilder);

        KStream<String, String> inputStream = builder.stream("input-topic");

        KStream<String, String> deduplicatedStream = inputStream.transformValues(
                () -> new DeduplicationTransformer(storeName),
                storeName);

        deduplicatedStream.filter((key, value) -> value != null).to("output-topic");

        return builder.build();
    }

}
