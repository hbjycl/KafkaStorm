
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.*;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.kafka.trident.TridentKafkaStateFactory;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import java.util.Properties;
import java.util.UUID;

public class StormKafkaTopo {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        BrokerHosts hosts = new ZkHosts("h1:2181,h2:2181,h3:2181");
        SpoutConfig spoutConfig = new SpoutConfig(hosts, "topic", "/storm", UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        builder.setSpout("spout", kafkaSpout, 5);


        Properties props = new Properties();
        props.put("bootstrap.servers", "h1:9092,h2:9092,h3:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaBolt bolt = new KafkaBolt().withProducerProperties(props);

        builder.setBolt("bolt", new SenqueceAfterBolt(),2).shuffleGrouping("spout");
        builder.setBolt("kafkabolt", bolt,2).shuffleGrouping("bolt");

        Config config = new Config();
        config.put("metadata.broker.list","h1:9092,h2:9092,h3:9092");
        config.put("key.serializer.class","kafka.serializer.StringEncoder");
        config.put("topic", "topic2");

        StormSubmitter.submitTopology(UUID.randomUUID().toString(), config, builder.createTopology());

        /*if (args != null && args.length > 0) {
            config.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("Topo", config, builder.createTopology());
            Utils.sleep(100000);
            cluster.killTopology("Topo");
            cluster.shutdown();
        }*/

    }
}