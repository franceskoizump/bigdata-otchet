package org.apache.storm.kafka.spout;

import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.EARLIEST;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.bolt.WordCountBolt;
import org.apache.storm.kafka.spout.Func;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.kafka.spout.trident.KafkaTridentSpoutOpaque;

import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;





public class WordsSpout {
    private static final String TOPIC_0_STREAM = "topic_0_stream";
    private static final String TOPIC_1 = "wordCount-topic";
    private static final String KAFKA_LOCAL_BROKER = "localhost:9092";

    protected KafkaSpoutConfig<String, String> newKafkaSpoutConfig() {
        ByTopicRecordTranslator<String, String> trans = new ByTopicRecordTranslator<>(
            (r) -> new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value()),
            new Fields("topic", "partition", "offset", "key", "value"), TOPIC_0_STREAM);
        return KafkaSpoutConfig.builder(KAFKA_LOCAL_BROKER, new String[]{TOPIC_1})
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutGroup")
                .setRetry(newRetryService())
                .setRecordTranslator(trans)
                .setOffsetCommitPeriodMs(10_000)
                .setFirstPollOffsetStrategy(EARLIEST)
                .setMaxUncommittedOffsets(250)
                .build();
    }

    protected StormTopology getTopologyKafkaSpout(KafkaSpoutConfig<String, String> spoutConfig) {
        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("KafkaSpout", new KafkaSpout<>(newKafkaSpoutConfig()), 1);

        tp.setBolt("PathChecjerBolt", new SplitSentence(), 1).shuffleGrouping("KafkaSpout", TOPIC_0_STREAM);
        tp.setBolt("StormCountBolt", new WordCountBolt(), 1).fieldsGrouping("PathChecjerBolt", new Fields("word"));


        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
            .setHost("127.0.0.1").setPort(6379).build();
        // Storm tuple to redis key-value mapper
        RedisStoreMapper storeMapper = new WordCountStoreMapper();

        tp.setBolt("RedisStoreBolt", new RedisStoreBolt(poolConfig, storeMapper), 1).shuffleGrouping("StormCountBolt");
        return tp.createTopology();
    }

    protected KafkaSpoutRetryService newRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(new TimeInterval(500L, TimeUnit.MICROSECONDS),
                TimeInterval.milliSeconds(2), Integer.MAX_VALUE, TimeInterval.seconds(10));
    }

    public static void main(String[] args) throws Exception {
        new WordsSpout().run(args);
    }

    protected void run(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, InterruptedException {
        Config tpConf = new Config();
        tpConf.setDebug(true);
        //Consumer. Sets up a topology that reads the given Kafka spout and counts words
        StormSubmitter.submitTopology(
            "KafkaSpout-StormBolt-WordCount", 
            tpConf, 
            getTopologyKafkaSpout(newKafkaSpoutConfig())
        );
    }


    
    public static class SplitSentence extends BaseRichBolt {
        private OutputCollector collector;

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            
            
            String path = input.getStringByField("value");
            try {
                
                Files.walk(Paths.get(path))
                    // .filter(Files::isRegularFile)
                    .forEach((pa) -> {
                        File f = new File(pa.toString());
                        String[] p = f.getName().split("\\.");
                        if (p.length > 1) {
                            collector.emit(input, new Values(p[p.length - 1]));
                        } else {
                            collector.emit(input, new Values("no format"));
                        }
                        //collector.emit(input, new Values(f.toString()));
                    });
            } catch (IOException e) {
                System.out.println(e.toString());
            }
            /*
            String[] words = sentence.split(" ");

            for (String word : words) {
                collector.emit(input, new Values(word));
            }
*/
            collector.ack(input);
        }


        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    // Maps a storm tuple to redis key and value
    private static class WordCountStoreMapper implements RedisStoreMapper {
        private final RedisDataTypeDescription description;
        private final String hashKey = "wordCountHashSet";

        WordCountStoreMapper() {
            description = new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.HASH, hashKey);
        }

        @Override
        public RedisDataTypeDescription getDataTypeDescription() {
            return description;
        }

        @Override
        public String getKeyFromTuple(ITuple tuple) {
            return tuple.getStringByField("word");
        }

        @Override
        public String getValueFromTuple(ITuple tuple) {
            return String.valueOf(tuple.getIntegerByField("count"));
        }
    }
}
