package com.wqj.hbase;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
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
	
	@After
	public void close() throws Exception {
		table.close();
		connection.close();
		
	}
}
