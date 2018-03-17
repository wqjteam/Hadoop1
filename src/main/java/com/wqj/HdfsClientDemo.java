package com.wqj;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;


public class HdfsClientDemo {

	FileSystem fileSystem =null;
	@Before
	public void init() throws Exception {
		Configuration conf = new Configuration();
		fileSystem = FileSystem.get(conf);
	}	
	
	@Test
	public void testUpload() throws Exception {
		fileSystem.copyFromLocalFile(new Path("E:\\测试数据.zip"), new Path("/测试数据.zip"));
		fileSystem.close();
	}
}
