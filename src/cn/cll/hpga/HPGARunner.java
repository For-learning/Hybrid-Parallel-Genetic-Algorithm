package cn.cll.hpga;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 用来描述一个作业job（使用哪个mapper类，哪个reducer类，输入文件在哪，输出结果放哪。。。。） 然后提交这个job给hadoop集群
 * 
 * @author duanhaitao@itcast.cn
 *
 */
// cn.itheima.bigdata.hadoop.mr.wordcount.WordCountRunner
public class HPGARunner {

	public static void main(String[] args) throws Exception {

		// ************************* 第一层MapReduce--生成初始种群
		// *****************************************
		Configuration conf = new Configuration();
		Job upperLayerJob = Job.getInstance(conf);
		// 设置job所使用的jar包
		conf.set("mapreduce.job.jar", "hpga.jar");

		// 设置job中的资源所在的jar包
		upperLayerJob.setJarByClass(HPGARunner.class);

		// 要使用哪个mapper类
		upperLayerJob.setMapperClass(UpperLayerMapper.class);
		upperLayerJob.setMapOutputKeyClass(Text.class);
		upperLayerJob.setMapOutputValueClass(Text.class);

		// 要使用哪个reducer类
		upperLayerJob.setReducerClass(UpperLayerReducer.class);
		upperLayerJob.setOutputKeyClass(Text.class);
		upperLayerJob.setOutputValueClass(LongWritable.class);

		// 指定要处理的原始数据所存放的路径
		FileInputFormat.setInputPaths(upperLayerJob, "hdfs://192.168.5.128:9000/hpga/ucityInput");
		// 指定处理之后的结果输出到哪个路径
		FileOutputFormat.setOutputPath(upperLayerJob, new Path("hdfs://192.168.5.128:9000/hpga/ucityOutput"));

		boolean res = upperLayerJob.waitForCompletion(true);

		System.exit(res ? 0 : 1);

		// ************************* 第一层MapReduce--生成初始种群
		// *****************************************
		// ************************* 第二层MapReduce--遗传算法进化
		// *****************************************

		// Configuration conf = new Configuration();
		// Job wcjob = Job.getInstance(conf);
		// // 设置job所使用的jar包
		// conf.set("mapreduce.job.jar", "wc.jar");
		//
		// // 设置wcjob中的资源所在的jar包
		// wcjob.setJarByClass(HPGARunner.class);
		//
		// // wcjob要使用哪个mapper类
		// wcjob.setMapperClass(LowerLayerMapper.class);
		// // wcjob要使用哪个reducer类
		// wcjob.setReducerClass(LowerLayerReducer.class);
		//
		// // wcjob的mapper类输出的kv数据类型
		// wcjob.setMapOutputKeyClass(Text.class);
		// wcjob.setMapOutputValueClass(LongWritable.class);
		//
		// // wcjob的reducer类输出的kv数据类型
		// wcjob.setOutputKeyClass(Text.class);
		// wcjob.setOutputValueClass(LongWritable.class);
		//
		// // 指定要处理的原始数据所存放的路径
		// FileInputFormat.setInputPaths(wcjob,
		// "hdfs://192.168.5.128:9000/wc/srcdata1");
		// // 指定处理之后的结果输出到哪个路径
		// FileOutputFormat.setOutputPath(wcjob, new
		// Path("hdfs://192.168.5.128:9000/wc/output1"));
		//
		// boolean res = wcjob.waitForCompletion(true);
		//
		// System.out.print("I am done");
		//
		// System.exit(res ? 0 : 1);

		// ************************* 第二层MapReduce--遗传算法进化
		// *****************************************

	}

}
