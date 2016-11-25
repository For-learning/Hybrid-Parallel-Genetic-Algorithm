package cn.cll.hpga;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UpperLayerMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// [55,66]#[23,45]#[24,45]#[11,78]#[34,25]
		// 获取到一行文件的内容
		String line = value.toString();
		// 切分这一行的内容为一个单词数组
		String[] words = StringUtils.split(line, "#");
		// 计算一共有多少个城市和组合数
		int cityAmount = words.length;
		int cityCombinaionAmount = 1;
		for (int i = 1; i <= cityAmount; i++) {
			cityCombinaionAmount = cityCombinaionAmount * i;
		}

		// 将数据中的数据都放在List集合中
		List<String> array = new ArrayList<String>();
		for (String word : words) {
			array.add(word);
		}

		// 根据组合数生成城市
		for (int j = 1; j <= cityCombinaionAmount; j++) {
			// 打乱顺序
			Collections.shuffle(array);
			// 转换成字符串写出去
			String outputStr = "";
			for (int z = 0; z < array.size(); z++) {
				if ((z + 1) == array.size())
					outputStr = outputStr + array.get(z);
				else
					outputStr = outputStr + array.get(z) + "#";
			}
			// 写到Reduce
			context.write(new Text(j + ""), new Text(outputStr));
		}

	}

}
