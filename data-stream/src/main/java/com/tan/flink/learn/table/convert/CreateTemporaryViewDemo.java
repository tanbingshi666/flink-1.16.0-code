package com.tan.flink.learn.table.convert;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * author name: tanbingshi
 * create time: 2022/11/21 10:23
 * describe content: flink-1.16.0-learn
 */
public class CreateTemporaryViewDemo {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // create some DataStream
        DataStream<Tuple2<Long, String>> dataStream = env.fromElements(
                Tuple2.of(12L, "Alice"),
                Tuple2.of(0L, "Bob"));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // === EXAMPLE 1 ===

        // register the DataStream as view "MyView" in the current session
        // all columns are derived automatically

        tableEnv.createTemporaryView("MyView1", dataStream);

        tableEnv.from("MyView1").printSchema();
        /**
         * (
         *   `f0` BIGINT NOT NULL,
         *   `f1` STRING
         * )
         */

        // === EXAMPLE 2 ===

        // register the DataStream as view "MyView" in the current session,
        // provide a schema to adjust the columns similar to `fromDataStream`

        // in this example, the derived NOT NULL information has been removed

        tableEnv.createTemporaryView(
                "MyView2",
                dataStream,
                Schema.newBuilder()
                        .column("f0", "BIGINT")
                        .column("f1", "STRING")
                        .build());

        tableEnv.from("MyView2").printSchema();
        /**
         * (
         *   `f0` BIGINT,
         *   `f1` STRING
         * )
         */

        // === EXAMPLE 3 ===

        // use the Table API before creating the view if it is only about renaming columns

        tableEnv.createTemporaryView(
                "MyView3",
                tableEnv.fromDataStream(dataStream).as("id", "name"));

        tableEnv.from("MyView3").printSchema();
        /**
         * (
         *   `id` BIGINT NOT NULL,
         *   `name` STRING
         * )
         */
    }

}
