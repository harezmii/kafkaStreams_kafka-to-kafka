import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaStream {

    public static void main(String[] args) {

        // Read Kafka To Write Kafka with KafkaStream

        // Propertis
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); //localhost or İpAdress vs.
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        System.out.println("Properties Ok");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, Object> suat = streamsBuilder.stream("topic"); // topic  is topic name
        System.out.println("Çalışıyor");

        suat.to("test"); // test is topic name
        System.out.println("Running");

        //  KafkaStream Topology
        final Topology topology = streamsBuilder.build();

        final KafkaStreams streams = new KafkaStreams(topology, props); // KafkaStream Object
        // 1 seconds
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
