package com.hsbc.hackathon.dao;

import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

public interface BigTableDao {
	public String[] queryForIR(byte[] rowKey);
	public List<byte[]> scanForIRKey(String ccy);
	public String[] queryForFX(byte[] rowKey);
	public List<byte[]> scanForFXKey(String ccy);
	public ResultScanner queryForTradeList();
	public Result queryForTradeAttributes (byte[] rowKey);
}
