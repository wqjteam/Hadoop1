package com.wqj;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;




/**
 * @Description: TODO
 * @author wqj
 * @date 2018年3月18日 下午8:36:53 
 */
public class HdfsClientDemo {
	
	public String uri = "hdfs://192.168.1.102:9000";
	
	FileSystem fileSystem =null;
	@Before
	public void init() throws Exception {
		Configuration conf = new Configuration();
		System.out.println(conf);
		fileSystem = FileSystem.get(URI.create(uri),conf);
	}	
	
	@Test
	public void testUpload() throws Exception {
		fileSystem.copyFromLocalFile(new Path("E:\\测试数据.zip"), new Path("/测试数据.zip"));
		fileSystem.close();
	}
	
}
