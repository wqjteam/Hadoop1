package com.wqj;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;

import javax.xml.transform.SourceLocator;

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
	
	
	FileSystem fileSystem =null;
	@Before
	public void init() throws Exception {
		Properties properties= new Properties();
		
		InputStream inputStream =ClassLoader.getSystemResourceAsStream("hadoop/hadoop.properties");
		properties.load(inputStream);
		System.out.println(properties);
		Configuration conf = new Configuration();
//		fileSystem = FileSystem.get(URI.create(uri),conf);
	}	
	
	@Test
	public void testUpload() throws Exception {
//		fileSystem.copyFromLocalFile(new Path("E:\\测试数据.zip"), new Path("/测试数据.zip"));
//		fileSystem.close();
	}
	
}
