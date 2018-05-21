package com.wqj.hdfs;

import java.io.InputStream;
import java.net.URI;
import java.util.Properties;

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
		String	uri = properties.getProperty("uri")+":"+properties.getProperty("port");
		Configuration conf = new Configuration();
		fileSystem = FileSystem.get(URI.create(uri),conf);
	}

	@Test
	public void testUpload() throws Exception {
		fileSystem.copyFromLocalFile(new Path("E:\\测试数据.zip"), new Path("/测试数据.zip"));
		fileSystem.close();
	}

	@Test
	public void testGet() throws Exception {
		fileSystem.copyToLocalFile(new Path("/测试数据.zip"), new Path("F:\\测试数据.zip"));
	}

	@Test
	public void testDelete() throws Exception {
		fileSystem.delete(new Path("/测试数据.zip"));
	}
}
