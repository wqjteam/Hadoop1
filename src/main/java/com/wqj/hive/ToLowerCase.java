package com.wqj.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

public class ToLowerCase extends UDF{

	public String evaluate(String field) {
		String lowerCase = field.toLowerCase();
		
		return lowerCase;
	}
}
