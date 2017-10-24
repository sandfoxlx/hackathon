package com.hsbc.hackathon.dao;

import java.math.BigDecimal;
import java.util.Date;

public interface BigQueryDao {
	public void persist(String tradeId, String cptyId, Integer scenario, Date timePoint, BigDecimal mtm);
}
