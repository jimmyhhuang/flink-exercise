package flink.demo.element;

import flink.demo.utils.CheckPointUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SupportedSavepointSource {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();

        // 设置从 savepoint 启动（如果 savepoint 存在）
        config.setString("execution.savepoint.path", "file:///Users/huangqi/Desktop/flink-exercise/checkpoint/element/99d6085341009f4afff15886d1d74772/chk-1");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1); // 设置并行度为1便于测试

        CheckPointUtils.configureCheckpoints(env, "file:///Users/huangqi/Desktop/flink-exercise/checkpoint/element");
        // 使用集合数据源（有限数据流，可以完成）
        DataStreamSource<String> stream = env.fromElements(
                "hello world",
                "hello flink",
                "flink streaming",
                "hello savepoint",
                "hello flink",
                "flink streaming",
                "hello savepoint",
                "flink checkpoint"
        );

        // 有状态处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCounts = stream
                .flatMap(new SlowTokenizer())
                .keyBy(value -> value.f0)
                .sum(1);

        wordCounts.print();

        env.execute("Supported Savepoint Example");
    }

    /**
     * 慢速 Tokenizer - 在每个元素处理时添加延迟
     */
    public static class SlowTokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            System.out.println("开始处理: " + value + " 时间: " + System.currentTimeMillis());

            // 添加处理延迟（5秒）
            Thread.sleep(5000);

            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }

            System.out.println("完成处理: " + value + " 时间: " + System.currentTimeMillis());
        }
    }
}