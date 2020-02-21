package com.sming.mobile;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MobilePartion extends Partitioner<Text, MobileFlow>{

	@Override
	public int getPartition(Text key, MobileFlow value, int numPartitions) {
		String move = "^((134)|(135)|(136)|(137)|(138)|(139)|(147)|(150)|(151)|(152)|(157)|(158)|(159)|(178)|(182)|(183)|(184)|(187)|(188)|(198))\\d{8}$";
		String link = "^((130)|(131)|(132)|(155)|(156)|(145)|(185)|(186)|(176)|(175)|(170)|(171)|(166))\\d{8}$";
		String telecom = "^((133)|(153)|(173)|(177)|(180)|(181)|(189)|(199))\\d{8}$";
		String s =key.toString();
		if (s.matches(move)) {
			return 0;
		}else if(s.matches(link)){
			return 1;
		}else if(s.matches(telecom)) {
			return 2;
		}else {
			return 3;
		}
	}
	
}
