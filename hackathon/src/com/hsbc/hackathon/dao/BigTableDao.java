package com.hsbc.hackathon.dao;

public interface BigTableDao {
	public String[] queryForIRArray(String columnFamilyName, String columnName);
	public String[] queryForFXArray(String columnFamilyName, String columnName);
	public String[] queryForTradeAttributes (String columnFamilyName, String columnName);
}
