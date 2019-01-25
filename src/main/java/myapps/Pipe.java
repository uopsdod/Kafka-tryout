package myapps;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

public class Pipe {
	public static void main(String[] args) throws Exception {
		
		final StreamsBuilder builder = new StreamsBuilder(); // create a topology builder
		
		// Now we get a KStream that is continuously generating records from its source Kafka topic streams-plaintext-input. The records are organized as String typed key-value pairs. 
		KStream<String, String> source = builder.stream("streams-plaintext-input"); // a node here 
		
		// The simplest thing we can do with this stream is to write it into another Kafka topic, say it's named streams-pipe-output
		source.to("streams-pipe-output"); // a node here too 
		
		// inspect what kind of topology is created from this builder
		final Topology topology = builder.build();
		System.out.println(topology.describe());
		
		
		//  construct the Streams client with the two components we have just constructed above
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
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
