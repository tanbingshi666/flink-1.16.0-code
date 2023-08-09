package com.tan.flink.learn.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * author name: tanbingshi
 * create time: 2022/11/16 14:33
 * describe content: flink-1.16.0-learn
 */
public class CustomSourceDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // windows 环境下 默认并行度等于 CPU 个数
//        env.setParallelism(1);

        env.addSource(new CustomSourceFunction())
                .forceNonParallel()
                .partitionCustom((key, numPartitions) -> key % numPartitions,
                        split -> split.bucket)
                .print();

        env.execute("Custom Source Job");
    }

    // 泛型 Event 表示 SourceFunction 输出结果类型
    static class CustomSourceFunction implements SourceFunction<Event> {

        private boolean running = true;

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {

            Random random = new Random();

            String[] users = {"Mary", "Alice", "Bob", "Cary"};
            String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};
            int[] buckets = {0, 1, 2, 3};

            while (running) {

                ctx.collect(new Event(
                        users[random.nextInt(users.length)],
                        urls[random.nextInt(urls.length)],
                        System.currentTimeMillis(),
                        buckets[random.nextInt(buckets.length)]
                ));

                Thread.sleep(1000);
            }

        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    static class Event {

        public Event(String user, String url, Long timestamp, int bucket) {
            this.user = user;
            this.url = url;
            this.timestamp = timestamp;
            this.bucket = bucket;
        }

        public String user;
        public String url;
        public Long timestamp;
        public int bucket;

        @Override
        public String toString() {
            return "Event{" +
                    "user='" + user + '\'' +
                    ", url='" + url + '\'' +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }

}
