package com.pc.demo.orchestrator.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;

import com.pc.demo.orchestrator.util.DBUtil;

@Service
public class DBService {

	private static final Logger logger = LogManager.getLogger(DBService.class);

	private static final Integer DB_PORT;
	private static final String DB_ENDPOINT = System.getenv("dbEndpoint");
	private static final String DB_REGION = System.getenv("region");
	private static final String DB_USER = System.getenv("userName");
	private static final String DB_SCHEMA = System.getenv("schema");
	private static final String DB_TABLE = System.getenv("table");
	private static final String DB_DATABASE = System.getenv("database");
	private static final long BACKOFF_TIME_MILLI = 1000; // One second

	static {
		DB_PORT = retrievePort("port", 5432);
	}

	private Connection conn;
	private DBUtil dbUtil;

	/**
	 * Constructor used in actual environment (inside Lambda handler).
	 */
	public DBService() {
	    logger.info("==== Environment Variables ====");
	    logger.info("DB_ENDPOINT: " + DB_ENDPOINT);
	    logger.info("DB_REGION: " + DB_REGION);
	    logger.info("DB_USER: " + DB_USER);
	    logger.info("DB_DATABASE: " + DB_DATABASE);
	    logger.info("DB_SCHEMA: " + DB_SCHEMA);
	    logger.info("DB_TABLE: " + DB_TABLE);
	    logger.info("==== Environment Variables ====");
		
		logger.info("Inside DBService Constructor()...");
		this.dbUtil = new DBUtil();
		//this.conn = dbUtil.createConnectionViaIamAuth(DB_USER, DB_ENDPOINT, DB_REGION, DB_DATABASE, DB_PORT);
		logger.info("Completed DBService Constructor()");
	}

	/**
	 * Get Data.
	 *
	 * @throws Exception 
	 */
	public void getData()
			throws Exception {
		logger.info("Inside getData()...");
		
		this.conn = refreshDbConnection();
		if (conn == null) {
			throw new Exception("Connection object could not be initialized !");
		}
		
		String query = "SELECT * FROM " + DB_SCHEMA + "." + DB_TABLE;
		PreparedStatement preparedStatement = conn.prepareStatement(query);
		logger.info("preparedStatement: " + preparedStatement.toString());

		ResultSet results = preparedStatement.executeQuery();
		while (results.next()) {
			logger.info("PK ID: " + results.getInt("id"));
		}
		
		logger.info("Completed getData()");
	}

	/**
	 * Refreshes the database connection in case there is a warm Lambda that has a
	 * connection that has either closed or failed to connect.
	 *
	 * @return the existing Connection or a new one in the case it needs to be
	 *         refreshed
	 */
	protected Connection refreshDbConnection() {
		logger.info("Inside refreshDbConnection()...");
		
		Connection connection = this.conn;
		try {
			if (connection == null || !connection.isValid(1)) {
				logger.info("Retrying database connection");
				try {
					Thread.sleep(BACKOFF_TIME_MILLI);
					connection = this.dbUtil.createConnectionViaIamAuth(DB_USER, DB_ENDPOINT, DB_REGION, DB_DATABASE, DB_PORT);
				} catch (InterruptedException e) {
					logger.error(e.getMessage(), e);
					throw new RuntimeException(
							"There was a problem sleeping the thread while creating a connection to the DB");
				}
			}

		} catch (SQLException e) {
			logger.error(e.getMessage(), e);
			throw new RuntimeException("There was a problem refreshing the database connection "
					+ "due to an error while checking validity");
		}

		logger.info("Completed refreshDbConnection()");
		return connection;
	}

	private static Integer retrievePort(String envVarName, Integer defaultPort) {
		logger.info("Inside retrievePort()...");
		
		Integer port = defaultPort;
		try {
			port = Integer.valueOf(System.getenv(envVarName));
		} catch (NumberFormatException nfe) {
			logger.warn("DB_PORT is not in environment variables or not an integer");
			port = defaultPort;
		}
		
		logger.info("Completed retrievePort()");
		return port;
	}
}
