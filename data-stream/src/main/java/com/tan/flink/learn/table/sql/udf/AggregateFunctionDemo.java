package com.tan.flink.learn.table.sql.udf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * author name: tanbingshi
 * create time: 2022/11/23 14:48
 * describe content: flink-1.16.0-learn
 */
public class AggregateFunctionDemo {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.createTemporarySystemFunction("temp_tbl_agg", CustomAggregateFunction.class);

        tableEnv.executeSql("CREATE TABLE socket_info (\n" +
                "  `user`         STRING,\n" +
                "  `score`        BIGINT,\n" +
                "  `weight`       INT\n" +
                ") WITH (\n" +
                "        'connector' = 'socket',\n" +
                "        'hostname' = 'hadoop',\n" +
                "        'port' = '10000',\n" +
                "        'format' = 'csv'\n" +
                ")");

        tableEnv.sqlQuery("SELECT `user`,temp_tbl_agg(score,weight) as res \n" +
                        "from socket_info group by user")
                .execute()
                .print();

    }

    public static class WeightedAvgAccumulator {
        public long sum = 0;
        public int count = 0;
    }

    public static class CustomAggregateFunction extends AggregateFunction<Long, WeightedAvgAccumulator> {

        @Override
        public Long getValue(WeightedAvgAccumulator acc) {
            if (acc.count == 0) {
                return null;
            }
            return acc.sum / acc.count;
        }

        @Override
        public WeightedAvgAccumulator createAccumulator() {
            return new WeightedAvgAccumulator();
        }

        public void accumulate(WeightedAvgAccumulator acc, Long value, Integer weight) {
            acc.sum += value * weight;
            acc.count += weight;
        }
    }

}
