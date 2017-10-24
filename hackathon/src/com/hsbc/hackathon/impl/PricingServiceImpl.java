package com.hsbc.hackathon.impl;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.hsbc.hackathon.PricingService;
import com.hsbc.hackathon.utils.BigTableHelper;

public class PricingServiceImpl implements PricingService {

	private static String PROJECT_ID = "riskhackathonproject2";
	private static String INSTANCE_ID = "hackathon";
	
	@Override
	public void runPricing() {
		BigTableHelper.init(PROJECT_ID, INSTANCE_ID);
	}

	
	public static void main(String[] args) {
		BigTableHelper.init(PROJECT_ID, INSTANCE_ID);
		Connection connection = BigTableHelper.getConnection();
		Table table;
		try {
			table = connection.getTable(TableName.valueOf("table1"));
			String rowKey = "c1";
			Result getResult = table.get(new Get(Bytes.toBytes(rowKey)));
			String greeting = Bytes.toString(getResult.getValue(Bytes.toBytes("column1"), Bytes.toBytes("c1")));
			System.out.println("Get a single greeting by row key");
			System.out.printf("\t%s = %s\n", rowKey, greeting);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
