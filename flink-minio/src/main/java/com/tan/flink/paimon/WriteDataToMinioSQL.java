package com.tan.flink.paimon;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class WriteDataToMinioSQL {

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

        tableEnv.executeSql("CREATE TEMPORARY TABLE datagen (\n" +
                " f_sequence INT,\n" +
                " f_random INT,\n" +
                " f_random_str STRING,\n" +
                " ts AS localtimestamp,\n" +
                " WATERMARK FOR ts AS ts\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                "\n" +
                " 'rows-per-second'='1',\n" +
                " \n" +
                " 'fields.f_sequence.kind'='sequence',\n" +
                " 'fields.f_sequence.start'='1',\n" +
                " 'fields.f_sequence.end'='100000',\n" +
                "\n" +
                " 'fields.f_random.min'='1',\n" +
                " 'fields.f_random.max'='100000',\n" +
                "\n" +
                " 'fields.f_random_str.length'='10'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS sink_minio (\n" +
                "    f_sequence INT,\n" +
                "    f_random INT,\n" +
                "    f_random_str STRING\n" +
                ") WITH (\n" +
                "    'bucket' = '2',\n" +
                "    'bucket-key' = 'f_random_str'\n" +
                ")");

        tableEnv.executeSql("insert into sink_minio select f_sequence, f_random, f_random_str from datagen");

    }

}
