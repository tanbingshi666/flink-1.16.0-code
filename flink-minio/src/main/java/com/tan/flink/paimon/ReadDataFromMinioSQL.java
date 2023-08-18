package com.tan.flink.paimon;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ReadDataFromMinioSQL {

    public static void main(String[] args) {

        Configuration conf = new Configuration();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        // checkpoint setting from: https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/dev/datastream/fault-tolerance/checkpointing/
        env.enableCheckpointing(60 * 1000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60 * 1000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // state backend setting from: https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/ops/state/state_backends/
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("s3a://flink/checkpoint");
        env.getConfig().setAutoWatermarkInterval(200L);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE CATALOG minio_catalog WITH (\n" +
                "    'type' = 'paimon',\n" +
                "    'warehouse' = 's3a://flink/paimon'\n" +
                ")");

        tableEnv.executeSql("USE CATALOG minio_catalog");

        tableEnv.sqlQuery("select * from sink_minio")
                .execute()
                .print();

    }

}
