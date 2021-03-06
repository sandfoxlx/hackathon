package com.hsbc.hackathon.utils;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class BigTableHelper {
	
	private static String PROJECT_ID = "riskhackathonproject2";
	private static String INSTANCE_ID = "hackathon";
	
	private static Connection connection = null;

	public static void connect() throws IOException {
		if (PROJECT_ID == null || INSTANCE_ID == null) {
			return;
		}
		connection = BigtableConfiguration.connect(PROJECT_ID, INSTANCE_ID);
	}

	public static synchronized Connection getConnection() {
		if (connection == null) {
			try {
				init(PROJECT_ID, INSTANCE_ID);
				connect();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return connection;
	}

	public static void init(String projectId, String instanceId) {
		PROJECT_ID = projectId;
		INSTANCE_ID = instanceId;
	}

	public void closeConnection() {
		if (connection == null) {
			return;
		}
		try {
			connection.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		connection = null;
	}
}