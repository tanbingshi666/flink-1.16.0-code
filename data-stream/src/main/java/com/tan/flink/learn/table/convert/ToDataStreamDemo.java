package com.tan.flink.learn.table.convert;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Instant;

/**
 * author name: tanbingshi
 * create time: 2022/11/21 10:29
 * describe content: flink-1.16.0-learn
 */
public class ToDataStreamDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql(
                "CREATE TABLE GeneratedTable "
                        + "("
                        + "  name STRING,"
                        + "  score INT,"
                        + "  event_time TIMESTAMP_LTZ(3),"
                        + "  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND"
                        + ")"
                        + "WITH ('connector'='datagen')");

        Table table = tableEnv.from("GeneratedTable");

        // === EXAMPLE 1 ===

        // use the default conversion to instances of Row

        // since `event_time` is a single rowtime attribute, it is inserted into the DataStream
        // metadata and watermarks are propagated

        DataStream<Row> dataStream1 = tableEnv.toDataStream(table);


        // === EXAMPLE 2 ===

        // a data type is extracted from class `User`,
        // the planner reorders fields and inserts implicit casts where possible to convert internal
        // data structures to the desired structured type

        // since `event_time` is a single rowtime attribute, it is inserted into the DataStream
        // metadata and watermarks are propagated

        DataStream<User> dataStream2 = tableEnv.toDataStream(table, User.class);

        // data types can be extracted reflectively as above or explicitly defined

        DataStream<User> dataStream3 =
                tableEnv.toDataStream(
                        table,
                        DataTypes.STRUCTURED(
                                User.class,
                                DataTypes.FIELD("name", DataTypes.STRING()),
                                DataTypes.FIELD("score", DataTypes.INT()),
                                DataTypes.FIELD("event_time", DataTypes.TIMESTAMP_LTZ(3))));
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class User {
        public String name;
        public Integer score;
        public Instant event_time;
    }

}
