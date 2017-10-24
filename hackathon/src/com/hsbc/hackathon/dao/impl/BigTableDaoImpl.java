package com.hsbc.hackathon.dao.impl;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.hsbc.hackathon.dao.BigTableDao;
import com.hsbc.hackathon.utils.BigTableHelper;

public class BigTableDaoImpl implements BigTableDao {

	private static String TABLE_FX = "FX";
	private static String TABLE_IR = "IR";
	private static String TABLE_TRADE = "TRADE";
	private static String CF_FX = "FX";
	private static String CF_IR = "IR";
	private static String CF_TRADE = "TRADE";

	@Override
	public String queryForIR(byte[] rowKey) {
		Connection connection = BigTableHelper.getConnection();
		Result result = null;
		try {
			Table table = connection.getTable(TableName.valueOf(TABLE_IR));
			result = table.get(new Get(rowKey));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return Bytes.toString(result.getValue(Bytes.toBytes(CF_IR), Bytes.toBytes("irRate")));
	}

	@Override
	public String queryForFX(byte[] rowKey) {
		Connection connection = BigTableHelper.getConnection();
		Result result = null;
		try {
			Table table = connection.getTable(TableName.valueOf(TABLE_FX));
			result = table.get(new Get(rowKey));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return Bytes.toString(result.getValue(Bytes.toBytes(CF_FX), Bytes.toBytes("fxRate")));
	}

	@Override
	public ResultScanner queryForTradeList() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Result queryForTradeAttributes(byte[] rowKey) {
		Connection connection = BigTableHelper.getConnection();
		Result result = null;
		try {
			Table table = connection.getTable(TableName.valueOf(TABLE_TRADE));
			result = table.get(new Get(rowKey));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public byte[] scanForFXKey(String ccy) {
		Connection connection = BigTableHelper.getConnection();
		Scan scan = new Scan();
		byte[] rowKey = null;
		try {
			Table table = connection.getTable(TableName.valueOf(TABLE_FX));
			ResultScanner scanner = table.getScanner(scan);
			for (Result row : scanner) {
				String toCcy = Bytes.toString(row.getValue(Bytes.toBytes(CF_FX), Bytes.toBytes("toCCY")));
				if (ccy.equalsIgnoreCase(toCcy)) {
					rowKey = row.getRow();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return rowKey;
	}

}
