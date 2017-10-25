package com.hsbc.hackathon.impl;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.hsbc.hackathon.PricingService;
import com.hsbc.hackathon.dao.BigQueryDao;
import com.hsbc.hackathon.dao.BigTableDao;
import com.hsbc.hackathon.dao.impl.BigQueryDaoImpl;
import com.hsbc.hackathon.dao.impl.BigTableDaoImpl;
import com.hsbc.hackathon.utils.BigTableHelper;
import com.hsbc.hackathon.utils.PricingUtils;

public class PricingServiceImpl implements PricingService {

	private BigTableDao bigTableDao = new BigTableDaoImpl();
	private BigQueryDao bigQueryDao = new BigQueryDaoImpl();
	
	@Override
	public void runPricingFX(byte[] rowKey) {

			Result getResult = bigTableDao.queryForTradeAttributes(rowKey);
			String tradeType = Bytes.toString(getResult.getValue(Bytes.toBytes("tradeAttribute"), Bytes.toBytes("tradeType")));
			String tradeId = Bytes.toString(getResult.getValue(Bytes.toBytes("tradeAttribute"), Bytes.toBytes("tradeId")));
			String cptyId = Bytes.toString(getResult.getValue(Bytes.toBytes("tradeAttribute"), Bytes.toBytes("cptyId")));
			if ("FX".equalsIgnoreCase(tradeType)) {
				String fromPrincipalStr = Bytes.toString(getResult.getValue(Bytes.toBytes("tradeAttribute"), Bytes.toBytes("fromPrincipal")));
				String toPrincipalStr = Bytes.toString(getResult.getValue(Bytes.toBytes("tradeAttribute"), Bytes.toBytes("toPrincipal")));
				String fromCcy = Bytes.toString(getResult.getValue(Bytes.toBytes("tradeAttribute"), Bytes.toBytes("fromCcy")));
				//Always USD, ignore
				//String toCcy = Bytes.toString(getResult.getValue(Bytes.toBytes("tradeAttribute"), Bytes.toBytes("toCcy")));
				BigDecimal fromPrincipal = new BigDecimal(fromPrincipalStr);
				BigDecimal toPrincipal = new BigDecimal(toPrincipalStr);
				List<byte[]> fxRowKeyList = bigTableDao.scanForFXKey(fromCcy);
				for (byte[] fxRowKey : fxRowKeyList) {
					String[] fxResult = bigTableDao.queryForFX(fxRowKey);
					BigDecimal pricingResult = PricingUtils.priceFXSwap(fromPrincipal, toPrincipal.divide(fromPrincipal, 6, RoundingMode.HALF_UP), BigDecimal.ONE, new BigDecimal("0.03"), new BigDecimal("0.04"), new BigDecimal(fxResult[1]), BigDecimal.ONE);
					bigQueryDao.persist(tradeId, cptyId, new Integer(fxResult[0]), "20171230", pricingResult);
				}
			} else if ("IR".equalsIgnoreCase(tradeType)) {
				String principal = Bytes.toString(getResult.getValue(Bytes.toBytes("tradeAttribute"), Bytes.toBytes("principal")));
				String fixIR = Bytes.toString(getResult.getValue(Bytes.toBytes("tradeAttribute"), Bytes.toBytes("fixIR")));
				String ccy = Bytes.toString(getResult.getValue(Bytes.toBytes("tradeAttribute"), Bytes.toBytes("ccy")));
				List<byte[]> irRowKeyList = bigTableDao.scanForIRKey(ccy);
				for (byte[] irRowKey : irRowKeyList) {
					String[] irResult = bigTableDao.queryForIR(irRowKey);
					BigDecimal pricingResult = PricingUtils.priceIRSwap(new BigDecimal(principal), new BigDecimal(irResult[1]), new BigDecimal(fixIR));
					bigQueryDao.persist(tradeId, cptyId, new Integer(irResult[0]), "20171230", pricingResult);
				}
			}
	}

	@Override
	public List<byte[]> getTradeListToPrice() {
		ResultScanner scanner = bigTableDao.queryForTradeList();
		List<byte[]> results = new ArrayList<>();
		for (Result row : scanner) {
			results.add(row.getRow());
		}
		return results;
	}
	
	
	public static void main(String[] args) {
		Connection connection = BigTableHelper.getConnection();
		Table table;
		try {
			table = connection.getTable(TableName.valueOf("table1"));
			String rowKey = "r1";
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
