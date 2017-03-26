package storm.T1;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import static storm.T1.utils.Utils.waitForSeconds;

public class WordCountTopology {

    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String COUNT_BOLT_ID = "count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "t1-topology";
    private static final String T1_TOPIC_SPOUT = "t1-topic-spout";

    public static void main(String[] args) throws Exception {

        ZkHosts zkHosts = new ZkHosts("192.168.1.16:2181");
        SpoutConfig spoutConfig = new SpoutConfig(
                zkHosts,
                "test",
                "/test",
                "test"
        );
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

//        SentenceSpout spout = new SentenceSpout();
        T1TopicSpout spout1  = new T1TopicSpout(spoutConfig);
        SplitSentenceBolt splitBolt = new SplitSentenceBolt();
        WordCountBolt countBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();


        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(T1_TOPIC_SPOUT, spout1);
        // SentenceSpout --> SplitSentenceBolt
//        builder.setBolt(SPLIT_BOLT_ID, splitBolt)
//                .shuffleGrouping(SENTENCE_SPOUT_ID);
//        // SplitSentenceBolt --> WordCountBolt
//        builder.setBolt(COUNT_BOLT_ID, countBolt)
//                .fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
//        // WordCountBolt --> ReportBolt
        builder.setBolt(REPORT_BOLT_ID, reportBolt);


        Config config = new Config();

//        LocalCluster cluster = new LocalCluster();
        StormSubmitter.submitTopology(TOPOLOGY_NAME,config,builder.createTopology());
//
//        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
//        waitForSeconds(10);
//        cluster.killTopology(TOPOLOGY_NAME);
//        cluster.shutdown();
    }
}
