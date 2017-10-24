package com.hsbc.hackathon.dao;

import java.math.BigDecimal;

public interface BigQueryDao {
	public void persist(String tradeId, String cptyId, Integer scenario, String timePoint, BigDecimal mtm);
}
