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
		// ��ȡ��һ���ļ�������
		String line = value.toString();
		// �з���һ�е�����Ϊһ����������
		String[] words = StringUtils.split(line, "#");
		// ����һ���ж��ٸ����к������
		int cityAmount = words.length;
		int cityCombinaionAmount = 1;
		for (int i = 1; i <= cityAmount; i++) {
			cityCombinaionAmount = cityCombinaionAmount * i;
		}

		// �������е����ݶ�����List������
		List<String> array = new ArrayList<String>();
		for (String word : words) {
			array.add(word);
		}

		// ������������ɳ���
		for (int j = 1; j <= cityCombinaionAmount; j++) {
			// ����˳��
			Collections.shuffle(array);
			// ת�����ַ���д��ȥ
			String outputStr = "";
			for (int z = 0; z < array.size(); z++) {
				if ((z + 1) == array.size())
					outputStr = outputStr + array.get(z);
				else
					outputStr = outputStr + array.get(z) + "#";
			}
			// д��Reduce
			context.write(new Text(j + ""), new Text(outputStr));
		}

	}

}
