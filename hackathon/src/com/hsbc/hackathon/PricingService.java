package com.hsbc.hackathon;

import java.util.List;

public interface PricingService {
	public void runPricingFX(byte[] rowKey);
	public List<byte[]> getTradeListToPrice();
}
