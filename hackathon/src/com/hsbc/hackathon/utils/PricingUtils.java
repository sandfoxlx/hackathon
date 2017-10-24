package com.hsbc.hackathon.utils;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class PricingUtils {
	public static BigDecimal priceFXSwap(BigDecimal fromPrincipal, BigDecimal fromCcyFXRate, BigDecimal toCcyFXRate, BigDecimal fromCcyIR, BigDecimal toCcyIR, BigDecimal fromCcyFutureFXRate, BigDecimal toCcyFutureFXRate) {
		BigDecimal toPrincipal = fromPrincipal.multiply(fromCcyFXRate).divide(toCcyFXRate, 6, RoundingMode.HALF_UP);
		BigDecimal pay = fromPrincipal.multiply(fromCcyIR).multiply(fromCcyFutureFXRate);
		BigDecimal receive = toPrincipal.multiply(toCcyIR).multiply(toCcyFutureFXRate);
		return receive.subtract(pay);
	}
	
	public static BigDecimal priceIRSwap(BigDecimal principal, BigDecimal floatIR, BigDecimal fixIR) {
		BigDecimal deltaIR = floatIR.subtract(fixIR);
		return principal .multiply(deltaIR);
	}
	
	public static void main(String[] args) {
		System.out.println("-- " + PricingUtils.priceFXSwap(new BigDecimal(10000), new BigDecimal("1.6"), new BigDecimal("1.2"), new BigDecimal("0.02"), new BigDecimal("0.03"), new BigDecimal("1.7"), new BigDecimal("1.1")));
		System.out.println("-- " + PricingUtils.priceFXSwap(new BigDecimal(10000), new BigDecimal("1.6"), new BigDecimal("1.2"), new BigDecimal("0.03"), new BigDecimal("0.02"), new BigDecimal("1.7"), new BigDecimal("1.1")));
		System.out.println("-- " + PricingUtils.priceFXSwap(new BigDecimal(10000), new BigDecimal("1.6"), new BigDecimal("1.2"), new BigDecimal("0.02"), new BigDecimal("0.03"), new BigDecimal("1.5"), new BigDecimal("1.3")));
		
		System.out.println("-- ir " + PricingUtils.priceIRSwap(new BigDecimal(10000), new BigDecimal("0.04"), new BigDecimal("0.04")));
		System.out.println("-- ir " + PricingUtils.priceIRSwap(new BigDecimal(10000), new BigDecimal("0.04"), new BigDecimal("0.03")));
		System.out.println("-- ir " + PricingUtils.priceIRSwap(new BigDecimal(10000), new BigDecimal("0.04"), new BigDecimal("0.06")));
	}
}
