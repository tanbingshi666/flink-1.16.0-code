package com.tan.flink.minio;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReadDataFromMinio {

    public static void main(String[] args) {

        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        // flink 表示 minio 的 bucket
        env.readTextFile("s3a://flink/checkpoint/")
                .print();

        try {
            env.execute("ooo");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
