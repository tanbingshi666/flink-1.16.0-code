package com.tan.flink.learn.table;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * author name: tanbingshi
 * create time: 2022/11/20 20:25
 * describe content: flink-1.16.0-learn
 */
public class TableDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1 读取数据源并标准化
        DataStream<Event> eventStream = env.socketTextStream("hadoop102", 10000)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Event(
                            fields[0],
                            fields[1],
                            Long.parseLong(fields[2])
                    );
                });

        // 2 获取流式 Table 执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 3 将 DataStream 转换为 Table
        Table eventTable = tableEnv.fromDataStream(eventStream);

        eventTable.printSchema();

        tableEnv.createTemporaryView("event", eventTable);

        // 4 查询
        Table visitTable = tableEnv.sqlQuery("select user,url from event");

        // 5 将 Table 转换为 DataStream 并输出
        tableEnv.toDataStream(visitTable).print();

        env.execute("Table Demo Job");
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Event {

        private String user;
        private String url;
        private Long ts;

        @Override
        public String toString() {
            return "Event{" +
                    "user='" + user + '\'' +
                    ", url='" + url + '\'' +
                    ", ts=" + ts +
                    '}';
        }
    }

}

