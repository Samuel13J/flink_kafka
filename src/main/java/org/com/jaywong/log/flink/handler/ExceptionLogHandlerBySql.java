package org.com.jaywong.log.flink.handler;

/**
 * @author wangjie
 * @create 2023-03-28 13:55
 */


import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.com.jaywong.log.utils.PropertiesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.com.jaywong.log.utils.PropertiesUtils;

public class ExceptionLogHandlerBySql {

    private static final Logger logger = LoggerFactory.getLogger(ExceptionLogHandlerBySql.class);

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        PropertiesUtils instance = PropertiesUtils.getInstance();
        //重启策略之固定间隔 (Fixed delay)
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(instance.getFlinkFixedDelayRestartTimes(),
                Time.of(instance.getFlinkFixedDelayRestartInterval(), TimeUnit.MINUTES)));

        //设置间隔多长时间产生checkpoint
        env.enableCheckpointing(instance.getFlinkCheckpointsInterval());
        //设置模式为exactly-once （这是默认值）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        // env.getCheckpointConfig().setMinPauseBetweenCheckpoints(instance.getFlinkMinPauseBetweenCheckpoints());
        //检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(instance.getFlinkCheckpointTimeout());
        //同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(instance.getFlinkMaxConcurrentCheckpoints());
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setParallelism(1);

        // flink table
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 构造 kafka source, 用 DEFAULT
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        String sourceDrop = "drop table if exists kafka_exception";
        String sourceTable = "CREATE TABLE kafka_exception ("
                + "  serviceId STRING,"
                + "  serverName STRING,"
                + "  serverIp STRING,"
                + "  title STRING,"
                + "  operationPath STRING,"
                + "  url STRING,"
                + "  stack STRING,"
                + "  exceptionName STRING,"
                + "  exceptionInfo STRING,"
                + "  operationUser STRING,"
                + "  operationIp STRING,"
                + "  orgId BIGINT,"
                + "  methodClass STRING,"
                + "  fileName STRING,"
                + "  methodName STRING,"
                + "  operationData STRING,"
                + "  occurrenceTime BIGINT"
                + ") WITH ("
                + "    'connector' = 'kafka',"
                + "    'topic' = '" + instance.getExceptionTopic() + "',"
                + "    'properties.bootstrap.servers' = '" + instance.getKafkServer() + "',"
                + "    'properties.group.id' = '" + instance.getGroupId() + "',"
                + "    'scan.startup.mode' = 'earliest-offset',"
                + "    'format' = 'json',"
                + "    'json.fail-on-missing-field' = 'false',"
                + "    'json.ignore-parse-errors' = 'true'"
                + "  )";
        System.out.println("=================sourcesql打印开始========================");
        tableEnv.executeSql(sourceDrop);
        tableEnv.executeSql(sourceTable);
        System.out.println(sourceTable);
        System.out.println("=================sourcesql打印结束========================");

        // 构造 hive catalog(这个可以任意编写)
        String name = "mycatalog";
        String defaultDatabase = instance.getDatabase();
        String hiveConfDir = instance.getHiveConf();
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog(name, hive);
        tableEnv.useCatalog(name);

        // hive sink
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.useDatabase(defaultDatabase);
        String sinkDrop = "drop table if exists hive_exception";
        String sinkTable = "CREATE TABLE hive_exception ("
                + "  service_id STRING,"
                + "  server_name STRING,"
                + "  server_ip STRING,"
                + "  title STRING,"
                + "  operation_path STRING,"
                + "  url STRING,"
                + "  stack STRING,"
                + "  exception_name STRING,"
                + "  exception_info STRING,"
                + "  operation_user STRING,"
                + "  operation_ip STRING,"
                + "  org_id BIGINT,"
                + "  method_class STRING,"
                + "  file_name STRING,"
                + "  method_name STRING,"
                + "  operation_data STRING,"
                + "  occurrence_time String"
                + " ) PARTITIONED BY (dt STRING) STORED AS parquet TBLPROPERTIES ("
                + "     'partition.time-extractor.timestamp-pattern'='$dt 00:00:00',"
                + "     'sink.partition-commit.trigger'='process-time',"
                + "     'sink.partition-commit.delay'='0 s',"
                + "     'sink.partition-commit.policy.kind'='metastore,success-file'"
                + ")";
        System.out.println("=================sinksql打印开始========================");
        tableEnv.executeSql(sinkDrop);
        tableEnv.executeSql(sinkTable);
        System.out.println(sinkTable);
        System.out.println("=================sinksql打印结束========================");
        String sql = "INSERT INTO TABLE hive_exception"
                + " SELECT serviceId, serverName, serverIp, title, operationPath, url, stack, exceptionName, exceptionInfo, operationUser, operationIp,"
                + " orgId, methodClass, fileName, methodName, operationData, from_unixtime(cast(occurrenceTime/1000 as bigint),'yyyy-MM-dd HH:mm:ss'), from_unixtime(cast(occurrenceTime/1000 as bigint),'yyyy-MM-dd')"
                + " FROM kafka_exception";
        tableEnv.executeSql(sql);
    }
}