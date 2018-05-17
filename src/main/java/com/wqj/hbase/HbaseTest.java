package com.wqj.hbase;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HbaseTest {

	/**
	 *   基本的配置信息
	 * */
	static Configuration  configuration=HBaseConfiguration.create();
	
	private Connection connection=null;
	
	private Table table=null;
	
	HBaseAdmin admin =null;
	
	@Before
	public void init() throws Exception {
		configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2");//zookeeeper 的配置
		configuration.set("hbase.zookeeper.property.clientPort", "2181"); //zookeeperde 端口
		connection = ConnectionFactory.createConnection(configuration);
		table = connection.getTable(TableName.valueOf("test3"));
		admin=new HBaseAdmin(configuration);
	}
	
	
	@Test
	public void createTable() throws Exception{
		
		//创建表描述类
		TableName tablename =TableName.valueOf("test3");
		HTableDescriptor descriptor=new HTableDescriptor(tablename);
		//创建列族的描述类
		HColumnDescriptor columnDescriptor=new HColumnDescriptor("info1");
		descriptor.addFamily(columnDescriptor);
		HColumnDescriptor columnDescriptor2=new HColumnDescriptor("info2");
		descriptor.addFamily(columnDescriptor2);
		
		//创建表
		admin.createTable(descriptor);
	} 
	
	@Test
	public void deleteTable() throws Exception {
		admin.disableTable("test3");
		admin.deleteTable("test3");
	}
	
	
	@Test
	public void insertData() throws Exception {
		
		Put put = new Put(Bytes.toBytes("zhangsanfen")); //列 row
		put.add(Bytes.toBytes("info1"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
		put.add(Bytes.toBytes("info1"), Bytes.toBytes("age"), Bytes.toBytes("18"));
		put.add(Bytes.toBytes("info1"), Bytes.toBytes("sex"), Bytes.toBytes("男"));
		put.add(Bytes.toBytes("info1"), Bytes.toBytes("dept"), Bytes.toBytes("物理"));
		table.put(put);
	}
	
	@Test
	public void deleteDate() throws IOException {
		Delete delete=new Delete(Bytes.toBytes("zhangsanfen"));
		delete.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("name"));
		table.delete(delete);
		
	}
	
	@Test
	public void queryDate() throws Exception {
		Get get = new Get(Bytes.toBytes("zhangsanfen"));
		
		System.out.println(table.get(get));
	}
	
	@Test
	public void scanData() throws Exception {
		
		Scan scan=new Scan();
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println(result);
		}
	}
	
	/**
	 * 过滤器扫描表:列值过滤器
	 * */
	@Test
	public void scanDataByFilter() throws Exception {
		//创建过滤器
		SingleColumnValueExcludeFilter singleColumnValueExcludeFilter = new SingleColumnValueExcludeFilter(Bytes.toBytes("info1"), Bytes.toBytes("name"), CompareOp.EQUAL,  Bytes.toBytes("zhangsan"));
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("name"));
		scan.setFilter(singleColumnValueExcludeFilter);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println(result);;
			
		}
		
		
		
	}
	
	/**
	 * 过滤器扫描表:列前缀过滤器
	 * */
	public void scanDataByFilter3() throws Exception {
		ColumnPrefixFilter columnPrefixFilter=new ColumnPrefixFilter(Bytes.toBytes("name"));
		Scan scan=new Scan();
		scan.setFilter(columnPrefixFilter);
		
		
		
		
		
	}
	
	/**
	 * 过滤器扫描表:rowkey过滤器(重点)
	 * */
	public void scanDataByFilter2() throws Exception {
		RowFilter filter=new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^wang"));
		Scan scan=new Scan();
		scan.setFilter(filter);
		ResultScanner scanner = table.getScanner(scan);
		
		
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	@After
	public void close() throws Exception {
		table.close();
		connection.close();
		
	}
}
