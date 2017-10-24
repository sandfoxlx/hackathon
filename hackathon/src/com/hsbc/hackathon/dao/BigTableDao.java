package com.hsbc.hackathon.dao;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

public interface BigTableDao {
	public String queryForIR(byte[] rowKey);
	public String queryForFX(byte[] rowKey);
	public byte[] scanForFXKey(String ccy);
	public ResultScanner queryForTradeList();
	public Result queryForTradeAttributes (byte[] rowKey);
}
