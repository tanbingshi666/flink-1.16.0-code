package com.tan.flink.learn.table.convert;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Instant;

import static org.apache.flink.table.api.Expressions.*;

/**
 * author name: tanbingshi
 * create time: 2022/11/21 11:02
 * describe content: flink-1.16.0-learn
 */
public class ToChangelogStreamDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // create Table with event-time
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

        // convert to DataStream in the simplest and most general way possible (no event-time)

        Table simpleTable = tableEnv
                .fromValues(
                        row("Alice", 12),
                        row("Alice", 2),
                        row("Bob", 12))
                .as("name", "score")
                .groupBy($("name"))
                .select($("name"), $("score").sum());

        tableEnv
                .toChangelogStream(simpleTable)
                .executeAndCollect()
                .forEachRemaining(System.out::println);
        /**
         * +I[Alice, 12]
         * -U[Alice, 12]
         * +U[Alice, 14]
         * +I[Bob, 12]
         */

        // === EXAMPLE 2 ===

        // convert to DataStream in the simplest and most general way possible (with event-time)

        DataStream<Row> dataStream2 = tableEnv.toChangelogStream(table);

        // since `event_time` is a single time attribute in the schema, it is set as the
        // stream record's timestamp by default; however, at the same time, it remains part of the Row

        dataStream2.process(
                new ProcessFunction<Row, Void>() {
                    @Override
                    public void processElement(Row row, Context ctx, Collector<Void> out) {
                        // prints: [name, score, event_time]
                        System.out.println(row.getFieldNames(true));

                        // timestamp exists twice
                        assert ctx.timestamp() == row.<Instant>getFieldAs("event_time").toEpochMilli();
                    }
                });

        // === EXAMPLE 3 ===

        // convert to DataStream but write out the time attribute as a metadata column which means
        // it is not part of the physical schema anymore

        DataStream<Row> dataStream3 = tableEnv.toChangelogStream(
                table,
                Schema.newBuilder()
                        .column("name", "STRING")
                        .column("score", "INT")
                        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                        .build());

        // the stream record's timestamp is defined by the metadata; it is not part of the Row

        dataStream3.process(
                new ProcessFunction<Row, Void>() {
                    @Override
                    public void processElement(Row row, Context ctx, Collector<Void> out) {

                        // prints: [name, score]
                        System.out.println(row.getFieldNames(true));

                        // timestamp exists once
                        System.out.println(ctx.timestamp());
                    }
                });

        // === EXAMPLE 4 ===

        // for advanced users, it is also possible to use more internal data structures for efficiency

        // note that this is only mentioned here for completeness because using internal data structures
        // adds complexity and additional type handling

        // however, converting a TIMESTAMP_LTZ column to `Long` or STRING to `byte[]` might be convenient,
        // also structured types can be represented as `Row` if needed

        DataStream<Row> dataStream4 = tableEnv.toChangelogStream(
                table,
                Schema.newBuilder()
                        .column(
                                "name",
                                DataTypes.STRING().bridgedTo(StringData.class))
                        .column(
                                "score",
                                DataTypes.INT())
                        .column(
                                "event_time",
                                DataTypes.TIMESTAMP_LTZ(3).bridgedTo(Long.class))
                        .build());

        // leads to a stream of Row(name: StringData, score: Integer, event_time: Long)

        dataStream4.process(
                new ProcessFunction<Row, Void>() {
                    @Override
                    public void processElement(Row row, Context ctx, Collector<Void> out) {

                        // prints: [name, score, event_time]
                        System.out.println(row.getFieldNames(true));
                    }
                });

        env.execute();
    }

}
