package com.tan.flink.learn.transformation.udf;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * author name: tanbingshi
 * create time: 2022/11/17 9:27
 * describe content: flink-1.16.0-learn
 */
public class Accumulator {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("hadoop", 10000);

        source.map(new RichMapFunction<String, String>() {

            // init
            private IntCounter numLines = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                // register
                getRuntimeContext().addAccumulator("num-lines", this.numLines);
            }

            @Override
            public String map(String in) throws Exception {
                // counter
                this.numLines.add(1);
                return in;
            }

            @Override
            public void close() throws Exception {
                // nothing to do
            }
        }).print();

        JobExecutionResult result = env.execute("Accumulator Job");

        Integer count = result.getAccumulatorResult("num-lines");

        System.out.println("count = " + count);
    }

}
