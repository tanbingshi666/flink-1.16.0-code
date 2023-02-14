package com.tan.flink.learn.table.convert;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/**
 * author name: tanbingshi
 * create time: 2022/11/21 10:51
 * describe content: flink-1.16.0-learn
 */
public class FromChangelogStreamDemo {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // EXAMPLE 1

        // create a changelog DataStream
        DataStream<Row> dataStream1 =
                env.fromElements(
                        Row.ofKind(RowKind.INSERT, "Alice", 12),
                        Row.ofKind(RowKind.INSERT, "Bob", 5),
                        Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 12),
                        Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100));

        // interpret the DataStream as a Table
        Table table1 = tableEnv.fromChangelogStream(dataStream1);

        // register the table under a name and perform an aggregation
        tableEnv.createTemporaryView("InputTable1", table1);
        tableEnv
                .executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable1 GROUP BY f0")
                .print();
        /**
         * +----+--------------------------------+-------------+
         * | op |                           name |       score |
         * +----+--------------------------------+-------------+
         * | +I |                          Alice |          12 |
         * | +I |                            Bob |           5 |
         * | -D |                          Alice |          12 |
         * | +I |                          Alice |         100 |
         * +----+--------------------------------+-------------+
         */

        // === EXAMPLE 2 ===

        // interpret the stream as an upsert stream (without a need for UPDATE_BEFORE)

        // create a changelog DataStream
        DataStream<Row> dataStream2 =
                env.fromElements(
                        Row.ofKind(RowKind.INSERT, "Alice", 12),
                        Row.ofKind(RowKind.INSERT, "Bob", 5),
                        Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100));

        // interpret the DataStream as a Table
        Table table2 =
                tableEnv.fromChangelogStream(
                        dataStream2,
                        Schema.newBuilder().primaryKey("f0").build(),
                        ChangelogMode.upsert());

        // register the table under a name and perform an aggregation
        tableEnv.createTemporaryView("InputTable2", table2);
        tableEnv
                .executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable2 GROUP BY f0")
                .print();
        /**
         * +----+--------------------------------+-------------+
         * | op |                           name |       score |
         * +----+--------------------------------+-------------+
         * | +I |                          Alice |          12 |
         * | +I |                            Bob |           5 |
         * | -U |                          Alice |          12 |
         * | +U |                          Alice |         100 |
         * +----+--------------------------------+-------------+
         */

    }

}
