package myapps;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;

public class LineSplit {
	public static void main(String[] args) throws Exception {
		
		final StreamsBuilder builder = new StreamsBuilder(); // create a topology builder
		
		KStream<String, String> source = builder.stream("streams-plaintext-input");
		KStream<String, String> words = source.flatMapValues(value -> Arrays.asList(value.split("\\W+")));
		
		words.to("streams-linesplit-output");
		
		// inspect what kind of topology is created from this builder
		final Topology topology = builder.build();
		System.out.println(topology.describe());
		
		//  construct the Streams client with the two components we have just constructed above
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-linesplit");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "52.38.105.107:2181");    // assuming that the Kafka broker this application is talking to runs on local machine with port 9092
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
