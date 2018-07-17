package com.wqj.hbase;

import com.wqj.hbase.util.DataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @Auther: wqj
 * @Date: 2018/7/11 14:39
 * @Description:
 */
public class HbaseTest {

    private Connection conn= DataSource.getConnection();
    public static void main(String[] args) throws SQLException {
       new HbaseTest().createTable();
    }

    /**
     * 创建表
     */
    public void createTable() throws SQLException {

        String sql="create table student('cf1'.'name' varchar null,'cf1'.'age' int null,'cf2'.'dept' varchar null)";
        Statement statement = conn.createStatement();
        statement.execute(sql);
        statement.close();
    }
    /**
    /**
     * 删除表
     */
    public void deleteTable(){

    }
    /**
     * 修改表
     */
    public void updateTable(){

    }

    /**
     * 查询表
     */
    public void getTable(){

    }
}
