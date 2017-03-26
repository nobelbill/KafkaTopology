package storm.T1;


import org.apache.storm.kafka.*;

/**
 * Created by nobelbill on 2017. 3. 26..
 */
public class T1TopicSpout extends KafkaSpout {
    private static final String TOPIC="test";

    public T1TopicSpout(SpoutConfig spoutConf) {
        super(spoutConf);
    }


}
