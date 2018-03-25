package com.wqj.mapreduces.phonehome;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * @Description: 自定义partitioner 将电话号码 在map计算完成后 将各地的电话号码输送到 
 *               各自的reducer  参数是map的输出类型
 * @author wqj
 * @date 2018年3月25日 下午1:30:37 
 */
//public class LocaltionPartioner implements Partitioner<Text, LocaltionBean>{
//
//	public void configure(JobConf arg0) {
//		// TODO Auto-generated method stub
//		
//	}
//
//	public int getPartition(Text arg0, LocaltionBean arg1, int arg2) {
//		// TODO Auto-generated method stub
//		return 0;
//	}
//
//}

public class LocaltionPartioner extends Partitioner<Text, LocaltionBean>{
	
	
	private static	Map<String,Integer> proviceDict = new HashMap<String,Integer>();
	/*将数据 加载指map中*/
	static {
		proviceDict.put("131", 1);
		proviceDict.put("132", 2);
		proviceDict.put("133", 3);
		proviceDict.put("134", 4);
		proviceDict.put("135", 5);
		proviceDict.put("136", 6);
		proviceDict.put("137", 7);
	}

	public int getPartition(Text arg0, LocaltionBean arg1, int arg2) {
		// TODO Auto-generated method stub
		/**
		 * 在此之前 ,应将点电话号码 对应的区域 填写指内存中 ,不应该去读数据库  效率低 使用静态块  
		 * 
		 * */
		String key = arg0.toString().substring(0, 3);
		
		Integer value = proviceDict.get(key)==null?8:proviceDict.get(key);
		
		return value;
	}

}
