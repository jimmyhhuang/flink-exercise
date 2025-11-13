package flink.demo.sockect;

import flink.demo.utils.CheckPointUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class SocketTextStreamWordCount {
	public static void main(String[] args) throws Exception {

		// 创建 streaming execution environment
		// 方法1：通过配置
		Configuration configuration = new Configuration();
		configuration.setInteger(RestOptions.PORT, 8081);      // Web UI 端口
        configuration.setBoolean(RestOptions.ENABLE_FLAMEGRAPH, true); // 启用火焰图
        configuration.setString(RestOptions.BIND_ADDRESS, "0.0.0.0"); // 允许外部访问

		//代码指定启动savepoint点
		configuration.setString("execution.savepoint-path", "file:///Users/huangqi/Desktop/flink-exercise/checkpoint/socket/27d666b97217aa0a1189d6522968374c/chk-1");
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        CheckPointUtils.configureCheckpoints(env, "file:///Users/huangqi/Desktop/flink-exercise/checkpoint/socket");
        // 获取数据
		DataStreamSource<String> stream = env.socketTextStream("localhost", 9000);

		// 计数
		SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stream.flatMap(new LineSplitter())
				.keyBy(0)
				.sum(1);

		sum.print();

		env.execute("Java WordCount from SocketTextStream Example");
	}

	public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
		@Override
		public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
			String[] tokens = s.toLowerCase().split("\\W+");

			for (String token : tokens) {
				if (token.length() > 0) {
					collector.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}
}

