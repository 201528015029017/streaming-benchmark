package flink.benchmark.utils;

import flink.benchmark.BenchmarkConfig;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.io.FileNotFoundException;

import redis.clients.jedis.Jedis;

public class RedisAnalyzeTool {

	public static class Result {
		DescriptiveStatistics latencies;
		SummaryStatistics throughputs;

		public Result(DescriptiveStatistics latencies, SummaryStatistics throughputs) {
			this.latencies = latencies;
			this.throughputs = throughputs;
		}
	}

	public static Result analyze(BenchmarkConfig config) throws FileNotFoundException {

		Jedis jedis = new Jedis(config.redisHost);

		DescriptiveStatistics latencies = new DescriptiveStatistics();
		SummaryStatistics throughputs = new SummaryStatistics();

		for (String campaign: jedis.smembers("campaigns")) {
			String windowsKey = jedis.hget(campaign, "windows");
			if (windowsKey == null) continue;
			long windowCount = jedis.llen(windowsKey);

			for (String windowTime: jedis.lrange(windowsKey, 0, windowCount)) {
				String windowKey = jedis.hget(campaign, windowTime);
				String seen = jedis.hget(windowKey, "seen_count");
				String timeUpdated = jedis.hget(windowKey, "time_updated");

				latencies.addValue(Long.parseLong(timeUpdated) - Long.parseLong(windowTime));
			}
		}

		return new Result(latencies, throughputs);
	}

	public static void main(String[] args) throws FileNotFoundException {
		BenchmarkConfig config = BenchmarkConfig.fromArgs(args);
		Result r1 = analyze(config);
		DescriptiveStatistics latencies = r1.latencies;
		SummaryStatistics throughputs = r1.throughputs;
		// System.out.println("lat-mean;lat-median;lat-90percentile;lat-95percentile;lat-99percentile;throughput-mean;throughput-max;latencies;throughputs;");
		System.out.println("all-machines;" + latencies.getMean() + ";" + latencies.getPercentile(50) + ";" + latencies.getPercentile(90) + ";" + latencies.getPercentile(95) + ";" + latencies.getPercentile(99) + ";" + latencies.getN());

		System.err.println("================= Latency  reports ) =====================");

		System.err.println("Mean latency " + latencies.getMean());
		System.err.println("Median latency " + latencies.getPercentile(50));
	}
}
