package com.huix.huixbigdata.huixlog;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * MR_WLA鐢ㄤ簬缃戠珯鏃ュ織鍒嗘瀽
 *
 * @className MR_WLA
 * @author mxlee
 * @email imxlee@foxmail.com
 * @date 2016骞�12鏈�2鏃�
 */
public class MR_WLA extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new MR_WLA(), args);
	}

	public int run(String[] args) throws Exception {
		String jobName = "wla_baidu";

		String inputPath = args[0];
		String outputPath = args[1];
		Path path = new Path(outputPath);
		// 鍒犻櫎杈撳嚭鐩綍
		path.getFileSystem(getConf()).delete(path, true);

		// 1銆佹妸鎵�鏈変唬鐮佺粍缁囧埌绫讳技浜嶵opology鐨勭被涓�
		Job job = Job.getInstance(getConf(), jobName);

		// 2銆佷竴瀹氳鎵撳寘杩愯锛屽繀椤诲啓涓嬮潰涓�琛屼唬鐮�
		job.setJarByClass(MR_WLA.class);

		// 3銆佹寚瀹氳緭鍏ョ殑hdfs
		FileInputFormat.setInputPaths(job, inputPath);

		// 4銆佹寚瀹歮ap绫�
		job.setMapperClass(WLA_Mapper.class);

		// 5銆佹寚瀹歮ap杈撳嚭鐨�<key,value>鐨勭被鍨�
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// 6銆佹寚瀹歳educe绫�
		job.setReducerClass(WLA_Reducer.class);

		// 7銆佹寚瀹歳educe杈撳嚭鐨�<key,value>鐨勭被鍨�
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 8銆佹寚瀹氳緭鍑虹殑hdfs
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * WLA_Mapper鐢ㄤ簬缃戠珯鏃ュ織鍒嗙粍
	 *
	 * @className WLA_Mapper
	 * @author mxlee
	 * @email imxlee@foxmail.com
	 * @date 2016骞�12鏈�2鏃�
	 */
	public static class WLA_Mapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// 鏍煎紡[2016-11-29 00:02:07 INFO ]
			// (cn.baidu.core.inteceptor.LogInteceptor:55) - [0 183.136.190.51
			// null http://www.baidu.cn/payment]
			String log = value.toString();// 缃戠珯璁块棶鏃ュ織
			String str = "(cn.baidu.core.inteceptor.LogInteceptor:55)";
			String baseUrl = "http://www.baidu.cn/";
			int len = str.length();
			int urlLen = baseUrl.length();
			if (log.indexOf(str) != -1) {
				String[] log1 = log.split(str);
				// 鍒嗘瀽绗竴娈礫2016-11-29 00:29:58 INFO
				String visitTime = log1[0].substring(1, 20);// 鑾峰彇璁块棶鏃堕棿
				// 鍒嗘瀽绗簩娈�112.90.82.196 null
				// http://www.baidu.cn/course/jobOffline]
				String[] split2 = log1[1].split("\t");
				String ip = split2[1];// 鑾峰彇ip
				String url = split2[3];// 鑾峰彇缃戝潃
				String subUrl = "http://www.baidu.cn";
				if (url.length() - 1 > urlLen) {
					subUrl = url.substring(urlLen, url.length() - 1);
				}
				String result = visitTime + "," + subUrl;
				context.write(new Text(ip), new Text(result));
			}
		}

	}

	/**
	 * WLA_Reducer鐢ㄤ簬澶勭悊鍒嗙粍鍚庣殑鏁版嵁
	 *
	 * @className WLA_Reducer
	 * @author mxlee
	 * @email imxlee@foxmail.com
	 * @date 2016骞�12鏈�2鏃�
	 */
	public static class WLA_Reducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			long firstTime = Long.MAX_VALUE;// 棣栨璁块棶鏃堕棿
			String startTime = null;
			String endTime = null;
			long lastTime = Long.MIN_VALUE;
			String firstPage = null;// 棣栨璁块棶椤甸潰
			String lastPage = null;
			int count = 0;// 璁块棶椤甸潰娆℃暟

			for (Text value : values) {
				count++;
				String[] split = value.toString().split(",");

				if (TimeUtil.transDate(split[0]) < firstTime) {
					firstTime = TimeUtil.transDate(split[0]);// yyyy-MM-dd
																// HH:mm:ss
					startTime = split[0].substring(11, 19);
					firstPage = split[1];
				}

				if (TimeUtil.transDate(split[0]) > lastTime) {
					lastTime = TimeUtil.transDate(split[0]);
					endTime = split[0].substring(11, 19);
					lastPage = split[1];
				}

			} // end for

			long time = 0;
			if ((lastTime - firstTime) % (1000 * 60) > 0) {
				time = (lastTime - firstTime) / (1000 * 60) + 1;
			} else {
				time = (lastTime - firstTime) / (1000 * 60);
			}
			String result = startTime + "\t" + firstPage + "\t" + endTime + "\t" + lastPage + "\t" + count + "\t" + time
					+ "鍒嗛挓";
			context.write(key, new Text(result));

		}// end reduce

	}// end class

}
