package myapps;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.state.KeyValueStore;

public class Wordcount {
	private static String APPLICATION_ID = "streams-wordcount";
	private static String BOOTSTRAP_SERVERS = "10.224.64.97:9092";

	public static void main(String[] args) throws Exception {

		final StreamsBuilder builder = new StreamsBuilder(); // create a topology builder

		KStream<String, String> source = builder.stream("streams-plaintext-input");

		KTable<String, Long> count = source.flatMapValues(
				new ValueMapper<String, Iterable<String>>() {
				@Override
				public Iterable<String> apply(String value) {
					return Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+"));
				}
				}).groupBy(new KeyValueMapper<String, String, String>() {
					@Override
					public String apply(String key, String value) {
						return value; // we only care about value in this demo 
					}
				})
				// Materialize the result into a KeyValueStore named "counts-store".
				// The Materialized store is always of type <Bytes, byte[]> as this is the
				// format of the inner most store.
				.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store")); // create a Count Processor with the store [counts-store]
		
		// convert KTable to Kstream(a changelog system) cuz the output topic is a changelog system 
		count.toStream().to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

		// inspect what kind of topology is created from this builder
		final Topology topology = builder.build();
		System.out.println(topology.describe());
		
		//  construct the Streams client with the two components we have just constructed above
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);    // assuming that the Kafka broker this application is talking to runs on local machine with port 9092
		props.put(StreamsConfig.STATE_DIR_CONFIG, "./tmp");
		
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		final KafkaStreams streams = new KafkaStreams(topology, props);
		addShutdownHook(streams);
	}

	private static void addShutdownHook(KafkaStreams streams) {

		final CountDownLatch latch = new CountDownLatch(1);

		// attach shutdown handler to catch control-c
		Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
			@Override
			public void run() {
				streams.close();
				latch.countDown();
			}
		});

		try {
			streams.start();
			latch.await();
		} catch (Throwable e) {
			System.exit(1);
		}
		System.exit(0);
	}
}
