package cn.cll.hpga;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * ��������һ����ҵjob��ʹ���ĸ�mapper�࣬�ĸ�reducer�࣬�����ļ����ģ����������ġ��������� Ȼ���ύ���job��hadoop��Ⱥ
 * 
 * @author duanhaitao@itcast.cn
 *
 */
// cn.itheima.bigdata.hadoop.mr.wordcount.WordCountRunner
public class HPGARunner {

	public static void main(String[] args) throws Exception {

		// ************************* ��һ��MapReduce--���ɳ�ʼ��Ⱥ
		// *****************************************
		Configuration conf = new Configuration();
		Job upperLayerJob = Job.getInstance(conf);
		// ����job��ʹ�õ�jar��
		conf.set("mapreduce.job.jar", "hpga.jar");

		// ����job�е���Դ���ڵ�jar��
		upperLayerJob.setJarByClass(HPGARunner.class);

		// Ҫʹ���ĸ�mapper��
		upperLayerJob.setMapperClass(UpperLayerMapper.class);
		upperLayerJob.setMapOutputKeyClass(Text.class);
		upperLayerJob.setMapOutputValueClass(Text.class);

		// Ҫʹ���ĸ�reducer��
		upperLayerJob.setReducerClass(UpperLayerReducer.class);
		upperLayerJob.setOutputKeyClass(Text.class);
		upperLayerJob.setOutputValueClass(LongWritable.class);

		// ָ��Ҫ�����ԭʼ��������ŵ�·��
		FileInputFormat.setInputPaths(upperLayerJob, "hdfs://192.168.5.128:9000/hpga/ucityInput");
		// ָ������֮��Ľ��������ĸ�·��
		FileOutputFormat.setOutputPath(upperLayerJob, new Path("hdfs://192.168.5.128:9000/hpga/ucityOutput"));

		boolean res = upperLayerJob.waitForCompletion(true);

		System.exit(res ? 0 : 1);

		// ************************* ��һ��MapReduce--���ɳ�ʼ��Ⱥ
		// *****************************************
		// ************************* �ڶ���MapReduce--�Ŵ��㷨����
		// *****************************************

		// Configuration conf = new Configuration();
		// Job wcjob = Job.getInstance(conf);
		// // ����job��ʹ�õ�jar��
		// conf.set("mapreduce.job.jar", "wc.jar");
		//
		// // ����wcjob�е���Դ���ڵ�jar��
		// wcjob.setJarByClass(HPGARunner.class);
		//
		// // wcjobҪʹ���ĸ�mapper��
		// wcjob.setMapperClass(LowerLayerMapper.class);
		// // wcjobҪʹ���ĸ�reducer��
		// wcjob.setReducerClass(LowerLayerReducer.class);
		//
		// // wcjob��mapper�������kv��������
		// wcjob.setMapOutputKeyClass(Text.class);
		// wcjob.setMapOutputValueClass(LongWritable.class);
		//
		// // wcjob��reducer�������kv��������
		// wcjob.setOutputKeyClass(Text.class);
		// wcjob.setOutputValueClass(LongWritable.class);
		//
		// // ָ��Ҫ�����ԭʼ��������ŵ�·��
		// FileInputFormat.setInputPaths(wcjob,
		// "hdfs://192.168.5.128:9000/wc/srcdata1");
		// // ָ������֮��Ľ��������ĸ�·��
		// FileOutputFormat.setOutputPath(wcjob, new
		// Path("hdfs://192.168.5.128:9000/wc/output1"));
		//
		// boolean res = wcjob.waitForCompletion(true);
		//
		// System.out.print("I am done");
		//
		// System.exit(res ? 0 : 1);

		// ************************* �ڶ���MapReduce--�Ŵ��㷨����
		// *****************************************

	}

}
