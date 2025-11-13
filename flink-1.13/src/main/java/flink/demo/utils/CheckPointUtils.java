package flink.demo.utils;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CheckPointUtils {


	/**
	 * 配置 Checkpoint 保存到mac指定目录下
	 */
	public static void configureCheckpoints(StreamExecutionEnvironment env, String checkpointPath) {
		//默认checkpoint功能是disabled的，想要使用的时候需要先启用
		// 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(1000);

		// 高级选项配置
		CheckpointConfig checkpointConfig = env.getCheckpointConfig();
		checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		// 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
		checkpointConfig.setMinPauseBetweenCheckpoints(500);
		// 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
		checkpointConfig.setCheckpointTimeout(60000);
		// 同一时间只允许进行一个检查点
		checkpointConfig.setMaxConcurrentCheckpoints(1);
		/**
		 * ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
		 * ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
		 */
		checkpointConfig.enableExternalizedCheckpoints(
				CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
		);

		// 设置 checkpoint 存储路径
		checkpointConfig.setCheckpointStorage(checkpointPath);

		// 可选：设置重启策略
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));
	}

}
