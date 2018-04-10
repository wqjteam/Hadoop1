package com.wqj.mapreduces.together;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.SetUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class ShareFriend {
	
	
	static class Map1 extends Mapper<LongWritable, Text, Text, Text>{

		Text text1=new Text();
		Text text2=new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			String line = value.toString();
			
			String[] split = line.split(":");
			
			String friend = split[0];
			
			String[] split2 = split[1].split(",");
			
			for (String string : split2) {
				text1.set(string);
				text2.set(friend);
				context.write(text1, text2);
			}
			
		}
		
		
		
	}
	
	
	static class Reducer1 extends Reducer<Text, Text, Text, Text> {

		Text text1=new Text();
		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1, Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			String str ="";
			for (Text text : arg1) {
				str+=text+",";
			}
			if(str.length()!=0) {
				str.substring(0, str.length()-1);
			}
			text1.set(str);
			arg2.write(arg0, text1);
		}
		
		
		
		
	}
	
	static class Map2 extends Mapper<LongWritable, Text, Text, Text>{

		Text text1=new Text();
		Text text2=new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			String line = value.toString();
			
			String[] split1 = line.split(" ");
			
			String	frind = split1[0];
			
			String[] split2 = split1[0].split(",");
			
			List<String> asList = Arrays.asList(split2);
			
			Collections.sort(asList);
			
			text2.set(frind);
			for (int i=0;i<asList.size();i++) {
				
				for (int j = i+1; j < asList.size()-1; j++) {
					text1.set(asList.get(i)+"-"+asList.get(j));
					
					context.write(text1, text2);
				}
			}
			
			
			
			
		}
		
		
		
		
	}
	
	
	
	static class Reducer2 extends Reducer<Text, Text, Text, Text>{

		Text text1=new Text();
		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, Text>.Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			Set<String> set =new HashSet<String>();
			for (Text text : arg1) {
				set.add(text.toString());
			}
			String str= "";
			for (String string : set) {
				str+=string+",";
			}
			if(str.length()!=0) {
				str.substring(0, str.length()-1);
			}
			text1.set(str);
			arg2.write(arg0,text1);
			
		}
		
		
		
	}
	
	
	public static void main(String[] args) {
	}
	
	
	
	
	
	
	
}
