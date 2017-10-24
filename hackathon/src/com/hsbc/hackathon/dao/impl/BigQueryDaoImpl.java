package com.hsbc.hackathon.dao.impl;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.api.client.util.DateTime;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import com.hsbc.hackathon.dao.BigQueryDao;

public class BigQueryDaoImpl implements BigQueryDao {

	private static String PROJECT_ID = "riskhackathonproject2";
	private static String DATASET_NAME = "hackathon";
	private static String TABLE_NAME = "PricingResult";
	@Override
	public void persist(String tradeId, String cptyId, Integer scenario, String timePoint, BigDecimal mtm) {
		TableId tableId = TableId.of(PROJECT_ID, DATASET_NAME, TABLE_NAME);
		// Values of the row to insert
		Map<String, Object> rowContent = new HashMap<>();
		rowContent.put("TradeId", tradeId);
		rowContent.put("CptyId", cptyId);
		rowContent.put("Scenario", scenario);
		rowContent.put("TimePoint", timePoint);
		rowContent.put("MTM", mtm);
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

		InsertAllResponse response = bigquery.insertAll(InsertAllRequest.newBuilder(tableId)
		    .addRow(tradeId, rowContent)
		    .build());
		if (response.hasErrors()) {
		  for (Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
			  System.err.println(entry);
		  }
		}
	}
	
	public static void main(String[] args) {
		BigQueryDao dao = new BigQueryDaoImpl();
		dao.persist("123", "HSBC", 1, "20171230", new BigDecimal("1000"));
	}
}
