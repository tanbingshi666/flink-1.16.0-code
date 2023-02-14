package com.tan.flink.learn.table.sql.udf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

/**
 * author name: tanbingshi
 * create time: 2022/11/23 15:13
 * describe content: flink-1.16.0-learn
 */
public class TableAggregateFunctionDemo {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.createTemporarySystemFunction("top2", CustomTableAggregateFunction.class);


    }

    public static class Top2Accumulator {
        public Integer first;
        public Integer second;
    }

    public static class CustomTableAggregateFunction extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2Accumulator> {

        @Override
        public Top2Accumulator createAccumulator() {
            Top2Accumulator acc = new Top2Accumulator();
            acc.first = Integer.MIN_VALUE;
            acc.second = Integer.MIN_VALUE;

            return acc;
        }

        public void accumulate(Top2Accumulator acc, Integer in) {
            if (in > acc.first) {
                acc.second = acc.first;
                acc.first = in;
            } else if (in > acc.second) {
                acc.second = in;
            }
        }

        public void emitValue(Top2Accumulator acc, Collector<Tuple2<Integer, Integer>>
                out) {
            if (acc.first != Integer.MIN_VALUE) {
                out.collect(new Tuple2<>(acc.first, 1));
            }
            if (acc.second != Integer.MIN_VALUE) {
                out.collect(new Tuple2<>(acc.second, 2));
            }
        }

    }

}
