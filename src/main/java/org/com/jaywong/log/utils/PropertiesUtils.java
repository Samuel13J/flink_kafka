package org.com.jaywong.log.utils;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author wangjie
 * @create 2023-03-28 13:51
 */
public class PropertiesUtils {

    private static Logger logger = LoggerFactory.getLogger(PropertiesUtils.class);
    private static PropertiesUtils instance = null;
    /** 间隔xxx秒产生checkpoing **/
    private Integer flinkCheckpointsInterval = null;
    /** 确保检查点之间有至少xxx ms的间隔 **/
    private Integer flinkMinPauseBetweenCheckpoints = null;
    /** 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】 **/
    private Integer flinkCheckpointTimeout = null;
    /** 同一时间只允许进行一个检查点 **/
    private Integer flinkMaxConcurrentCheckpoints = null;
    /** 尝试重启次数 **/
    private Integer flinkFixedDelayRestartTimes = null;
    /** 每次尝试重启时之间的时间间隔 **/
    private Integer flinkFixedDelayRestartInterval = null;
    /** kafka source 的并行度 **/
    private Integer kafkaSourceParallelism = null;
    /** hive sink 的并行度 **/
    private Integer hiveSinkParallelism = null;
    /** kafka集群 **/
    private String kafkServer = null;
    /** 消费者组id **/
    private String groupId = null;
    private Boolean enableAutoCommit = null;
    private Long autoCommitInterval = null;
    private String keyDeserializer = null;
    private String valueDeserializer = null;
    private String exceptionTopic = null;
    private String logTopic = null;
    private String hiveConf = null;
    private String database = null;

    /**
     * 静态代码块
     */
    private PropertiesUtils() {
        InputStream in = null;
        try {
            // 读取配置文件,通过类加载器的方式读取属性文件
            in = PropertiesUtils.class.getClassLoader().getResourceAsStream("project-config-test.properties");
//            in = PropertiesUtils.class.getClassLoader().getResourceAsStream("test-win10.properties");
//            in = PropertiesUtils.class.getClassLoader().getResourceAsStream("test-linux.properties");
            Properties prop = new Properties();
            prop.load(in);
            // flink配置
            flinkCheckpointsInterval = Integer.parseInt(prop.getProperty("flink.checkpoint.interval").trim());
            flinkMinPauseBetweenCheckpoints =
                    Integer.parseInt(prop.getProperty("flink.checkpoint.minPauseBetweenCheckpoints").trim());
            flinkCheckpointTimeout = Integer.parseInt(prop.getProperty("flink.checkpoint.checkpointTimeout").trim());
            flinkMaxConcurrentCheckpoints =
                    Integer.parseInt(prop.getProperty("flink.checkpoint.maxConcurrentCheckpoints").trim());
            flinkFixedDelayRestartTimes = Integer.parseInt(prop.getProperty("flink.fixedDelayRestart.times").trim());
            flinkFixedDelayRestartInterval =
                    Integer.parseInt(prop.getProperty("flink.fixedDelayRestart.interval").trim());
            kafkaSourceParallelism = Integer.parseInt(prop.getProperty("flink.kafka.source.parallelism").trim());
            hiveSinkParallelism = Integer.parseInt(prop.getProperty("flink.hive.sink.parallelism").trim());

            // kafka配置
            kafkServer = prop.getProperty("kafka.bootstrap.servers").trim();
            groupId = prop.getProperty("kafka.consumer.group.id").trim();
            enableAutoCommit = Boolean.valueOf(prop.getProperty("kafka.consumer.enableAutoCommit").trim());
            autoCommitInterval = Long.valueOf(prop.getProperty("kafka.consumer.autoCommitInterval").trim());
            keyDeserializer = prop.getProperty("kafka.consumer.key.deserializer").trim();
            valueDeserializer = prop.getProperty("kafka.consumer.value.deserializer").trim();
            exceptionTopic = prop.getProperty("kafka.exception.topic").trim();
            logTopic = prop.getProperty("kafka.log.topic").trim();

            hiveConf = prop.getProperty("hive.conf").trim();
            database = prop.getProperty("hive.database").trim();
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("流关闭失败");
            }
        }
    }

    public static PropertiesUtils getInstance() {
        if (instance == null) {
            instance = new PropertiesUtils();
        }
        return instance;
    }

    public Integer getFlinkCheckpointsInterval() {
        return flinkCheckpointsInterval;
    }

    public Integer getFlinkMinPauseBetweenCheckpoints() {
        return flinkMinPauseBetweenCheckpoints;
    }

    public Integer getFlinkCheckpointTimeout() {
        return flinkCheckpointTimeout;
    }

    public Integer getFlinkMaxConcurrentCheckpoints() {
        return flinkMaxConcurrentCheckpoints;
    }

    public Integer getFlinkFixedDelayRestartTimes() {
        return flinkFixedDelayRestartTimes;
    }

    public Integer getFlinkFixedDelayRestartInterval() {
        return flinkFixedDelayRestartInterval;
    }

    public Integer getKafkaSourceParallelism() {
        return kafkaSourceParallelism;
    }

    public Integer getHiveSinkParallelism() {
        return hiveSinkParallelism;
    }

    public String getKafkServer() {
        return kafkServer;
    }

    public String getGroupId() {
        return groupId;
    }

    public Boolean getEnableAutoCommit() {
        return enableAutoCommit;
    }

    public Long getAutoCommitInterval() {
        return autoCommitInterval;
    }

    public String getKeyDeserializer() {
        return keyDeserializer;
    }

    public String getValueDeserializer() {
        return valueDeserializer;
    }

    public String getExceptionTopic() {
        return exceptionTopic;
    }

    public String getLogTopic() {
        return logTopic;
    }

    public String getHiveConf() {
        return hiveConf;
    }

    public String getDatabase() {
        return database;
    }
}