package com.tan.flink.learn.state;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * author name: tanbingshi
 * create time: 2022/11/18 14:54
 * describe content: flink-1.16.0-learn
 * <p>
 * tan,aaa,1668670200000
 * tan,bbb,1668670201000
 * tan,ccc,1668670203000
 * tan,ddd,1668670204000
 * tan,eee,1668670205000
 * <p>
 * tan,red
 * tan,green
 */
public class BroadcastConnectedStreamDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Item> source1 = env.socketTextStream("hadoop", 10000)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Item(fields[0], fields[1], Long.parseLong(fields[2]));
                });

        SingleOutputStreamOperator<Rule> source2 = env.socketTextStream("hadoop", 10001)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Rule(fields[0], fields[1]);
                });

        // 一个 map descriptor，它描述了用于存储规则名称与规则本身的 map 存储结构
        MapStateDescriptor<String, Rule> ruleStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<Rule>() {
                }));

        // 广播流，广播规则并且创建 broadcast state
        BroadcastStream<Rule> ruleBroadcastStream = source2
                .broadcast(ruleStateDescriptor);

        // keyBy -> source1
        KeyedStream<Item, String> keyByStream = source1.keyBy(Item::getShape);

        keyByStream.connect(ruleBroadcastStream)
                .process(new CustomKeyedBroadcastProcessFunction(ruleStateDescriptor))
                .print();

        env.execute("Broadcast Connected Stream Demo Job");
    }

    //   KeyedBroadcastProcessFunction 中的类型参数表示：
    //   1. key stream 中的 key 类型
    //   2. 非广播流中的元素类型
    //   3. 广播流中的元素类型
    //   4. 结果的类型，在这里是 string
    static class CustomKeyedBroadcastProcessFunction extends KeyedBroadcastProcessFunction<String, Item, Rule, String> {

        // 存储部分匹配的结果，即匹配了一个元素，正在等待第二个元素
        // 我们用一个数组来存储，因为同时可能有很多第一个元素正在等待
        private final MapStateDescriptor<String, List<Item>> mapStateDesc =
                new MapStateDescriptor<>(
                        "items",
                        BasicTypeInfo.STRING_TYPE_INFO,
                        new ListTypeInfo<>(Item.class));

        // 与之前的 ruleStateDescriptor 相同
        private final MapStateDescriptor<String, Rule> ruleStateDescriptor;

        public CustomKeyedBroadcastProcessFunction(MapStateDescriptor<String, Rule> ruleStateDescriptor) {
            this.ruleStateDescriptor = ruleStateDescriptor;
        }

        @Override
        public void processElement(Item item,
                                   KeyedBroadcastProcessFunction<String, Item, Rule, String>.ReadOnlyContext ctx,
                                   Collector<String> out) throws Exception {

            System.out.println("process element is coming  with " + item);

            // get cache items
            final MapState<String, List<Item>> state = getRuntimeContext().getMapState(mapStateDesc);

            final String shape = item.getShape();

            // foreach rules
            for (Map.Entry<String, Rule> entry :
                    ctx.getBroadcastState(ruleStateDescriptor).immutableEntries()) {

                final String ruleShape = entry.getKey();
                final Rule rule = entry.getValue();

                List<Item> stored = state.get(ruleShape);
                if (stored == null) {
                    stored = new ArrayList<>();
                }

                // Items match Rule
                if (Objects.equals(shape, rule.getShape()) && !stored.isEmpty()) {
                    for (Item i : stored) {
                        out.collect("MATCH: " + i + " - " + rule);
                    }
                    stored.clear();
                }

                if (shape.equals(rule.getShape())) {
                    stored.add(item);
                }

                if (stored.isEmpty()) {
                    state.remove(ruleShape);
                } else {
                    state.put(ruleShape, stored);
                }
            }
        }

        @Override
        public void processBroadcastElement(Rule rule,
                                            KeyedBroadcastProcessFunction<String, Item, Rule, String>.Context context,
                                            Collector<String> collector) throws Exception {
            System.out.println("broadcast is coming with " + rule);

            // add broadcast element
            context.getBroadcastState(ruleStateDescriptor).put(rule.getShape(), rule);
        }

        @Override
        public void onTimer(long timestamp,
                            KeyedBroadcastProcessFunction<String, Item, Rule, String>.OnTimerContext ctx,
                            Collector<String> out) throws Exception {
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class Item {
        public String shape;
        public String name;
        public long ts;

        public String toString() {
            return "shape = " + getShape() + "\t name = " + getName() + "\t ts = " + getTs();
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class Rule {
        public String shape;
        public String color;

        public String toString() {
            return "shape = " + getShape() + "\t color = " + getColor();
        }
    }

}
