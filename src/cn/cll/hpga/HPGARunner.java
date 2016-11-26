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
		// Configuration conf = new Configuration();
		// Job upperLayerJob = Job.getInstance(conf);
		// // ����job��ʹ�õ�jar��
		// conf.set("mapreduce.job.jar", "hpga.jar");
		//
		// // ����job�е���Դ���ڵ�jar��
		// upperLayerJob.setJarByClass(HPGARunner.class);
		//
		// // Ҫʹ���ĸ�mapper��
		// upperLayerJob.setMapperClass(UpperLayerMapper.class);
		// upperLayerJob.setMapOutputKeyClass(Text.class);
		// upperLayerJob.setMapOutputValueClass(Text.class);
		//
		// // Ҫʹ���ĸ�reducer��
		// upperLayerJob.setReducerClass(UpperLayerReducer.class);
		// upperLayerJob.setOutputKeyClass(Text.class);
		// upperLayerJob.setOutputValueClass(LongWritable.class);
		//
		// // ָ��Ҫ�����ԭʼ��������ŵ�·��
		// FileInputFormat.setInputPaths(upperLayerJob,
		// "hdfs://192.168.5.128:9000/hpga/ucityInput");
		// // ָ������֮��Ľ��������ĸ�·��
		// FileOutputFormat.setOutputPath(upperLayerJob, new
		// Path("hdfs://192.168.5.128:9000/hpga/ucityOutput"));
		//
		// boolean res = upperLayerJob.waitForCompletion(true);
		//
		// System.exit(res ? 0 : 1);

		// ************************* ��һ��MapReduce--���ɳ�ʼ��Ⱥ
		// ************************* �ڶ���MapReduce--�Ŵ��㷨����

		Configuration conf = new Configuration();
		Job lowerLayerJob = Job.getInstance(conf);
		// ����job��ʹ�õ�jar��
		conf.set("mapreduce.job.jar", "hpga.jar");

		// ����job�е���Դ���ڵ�jar��
		lowerLayerJob.setJarByClass(HPGARunner.class);

		// Ҫʹ���ĸ�mapper��
		lowerLayerJob.setMapperClass(LowerLayerMapper.class);
		lowerLayerJob.setMapOutputKeyClass(LongWritable.class);
		lowerLayerJob.setMapOutputValueClass(Text.class);

		// Ҫʹ���ĸ�reducer��
		lowerLayerJob.setReducerClass(LowerLayerReducer.class);
		lowerLayerJob.setOutputKeyClass(Text.class);
		lowerLayerJob.setOutputValueClass(LongWritable.class);

		// ָ��Ҫ�����ԭʼ��������ŵ�·��
		FileInputFormat.setInputPaths(lowerLayerJob, "hdfs://192.168.5.128:9000/hpga/ucityOutput");
		// ָ������֮��Ľ��������ĸ�·��
		FileOutputFormat.setOutputPath(lowerLayerJob, new Path("hdfs://192.168.5.128:9000/hpga/firstGeneration"));

		boolean res = lowerLayerJob.waitForCompletion(true);

		System.exit(res ? 0 : 1);

		// ************************* �ڶ���MapReduce--�Ŵ��㷨����
		// *****************************************

	}

}
