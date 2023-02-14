package com.tan.flink.learn.transformation;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * author name: tanbingshi
 * create time: 2022/11/16 16:01
 * describe content: flink-1.16.0-learn
 */
public class AllWindow {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("hadoop", 10000);
        source.windowAll(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .reduce(new ReduceFunction<String>() {
                    @Override
                    public String reduce(String before, String in) throws Exception {
                        // 业务处理逻辑
                        return null;
                    }
                })
        // 第一个泛型 -> 输入元素类型
        // 第二个泛型 -> 触发窗口函数计算输出结果类型
        // 第三个泛型 -> 窗口类型
        // .apply(new AllWindowFunction<String, String, TimeWindow>() {
        //    @Override
        //    public void apply(TimeWindow timeWindow,
        //                      Iterable<String> iterable,
        //                      Collector<String> collector) throws Exception {
        //        // 业务处理逻辑
        //    }
        // })
        ;

    }

}
