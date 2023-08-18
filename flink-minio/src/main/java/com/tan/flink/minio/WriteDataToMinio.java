package com.tan.flink.minio;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 测试往 minio 写数据
 */
public class WriteDataToMinio {

    public static void main(String[] args) {

        Configuration conf = new Configuration();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        // checkpoint setting from: https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/dev/datastream/fault-tolerance/checkpointing/
        env.enableCheckpointing(10 * 1000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10 * 1000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // state backend setting from: https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/ops/state/state_backends/
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("s3://flink/checkpoint");
        env.getConfig().setAutoWatermarkInterval(200L);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE datagen (\n" +
                " f_sequence INT,\n" +
                " f_random INT,\n" +
                " f_random_str STRING,\n" +
                " ts AS localtimestamp,\n" +
                " WATERMARK FOR ts AS ts\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                "\n" +
                " 'rows-per-second'='5',\n" +
                " \n" +
                " 'fields.f_sequence.kind'='sequence',\n" +
                " 'fields.f_sequence.start'='1',\n" +
                " 'fields.f_sequence.end'='1000',\n" +
                "\n" +
                " 'fields.f_random.min'='1',\n" +
                " 'fields.f_random.max'='1000',\n" +
                "\n" +
                " 'fields.f_random_str.length'='10'\n" +
                ")");

        tableEnv.executeSql("select * from datagen")
                .print();
    }

}
