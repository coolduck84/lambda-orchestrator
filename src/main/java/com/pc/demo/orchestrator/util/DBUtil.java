package com.pc.demo.orchestrator.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import lombok.NonNull;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsUtilities;
import software.amazon.awssdk.services.rds.model.GenerateAuthenticationTokenRequest;

/**
 * Database Utility class. Used for connecting to the database and executing RDS
 * commands. It provide IAM Authentication mode. In the IAM Auth method, the
 * client doesn't need to know the password but it needs IAM permission to use
 * that User Name. RDS Proxy will fetch the password from a Secret Store which
 * is configured with RDS Proxy and accessible to the execution role.
 */
public class DBUtil {
	private static final String JDBC_PREFIX = "jdbc:postgresql://";
	private static final Logger logger = LogManager.getLogger(DBUtil.class);

	/**
	 * Creates a database connection via IAM Authentication. The password will be
	 * generated via an Authentication Token.
	 *
	 * @param userNameString username that the Lambda has access in IAM permissions
	 * @param dbEndpoint     RDS proxy endpoint
	 * @param region         RDS region
	 * @param port           RDS endpoint port
	 * @return a connection using IAM authentication
	 */
	public Connection createConnectionViaIamAuth(@NonNull String userNameString, @NonNull String dbEndpoint,
			@NonNull String region, @NonNull String db, Integer port) {
		logger.info("Inside createConnectionViaIamAuth()...");

		Connection connection;
		try {
			connection = DriverManager.getConnection(JDBC_PREFIX + dbEndpoint + "/" + db,
					setConnectionProperties(userNameString, dbEndpoint, region, port));

			logger.info("Completed createConnectionViaIamAuth()");
			return connection;
		} catch (Exception e) {
			logger.info("Connection FAILED in createConnectionViaIamAuth()");
			logger.error(e.getMessage(), e);
		}

		logger.info("Incomplete createConnectionViaIamAuth()");
		return null;
	}

	/**
	 * This method generates the IAM Authentication Token. The token will be later
	 * used as the password for authenticating to the DB.
	 *
	 * @return the authentication token
	 */
	public static String generateAuthToken(String username, String dbEndpoint, String region, Integer port) {
		RdsUtilities utilities = RdsUtilities.builder().credentialsProvider(DefaultCredentialsProvider.create())
				.region(Region.of(region)).build();
		logger.info("Inside generateAuthToken()...");

		GenerateAuthenticationTokenRequest authTokenRequest = GenerateAuthenticationTokenRequest.builder()
				.username(username).hostname(dbEndpoint).port(port).build();

		String authenticationToken = utilities.generateAuthenticationToken(authTokenRequest);

		logger.info("Completed generateAuthToken(): " + authenticationToken);
		return authenticationToken;
	}

	/**
	 * This method sets the connection properties, which includes the IAM Database
	 * Authentication token as the password. It also specifies that SSL verification
	 * is required.
	 *
	 * @param username   Username
	 * @param dbEndpoint Database endpoint
	 * @param region     AWS Region of the database
	 * @param port       Port for connecting to the endpoint
	 * @return connection properties
	 */
	private static Properties setConnectionProperties(String username, String dbEndpoint, String region, Integer port) {
		logger.info("Inside setConnectionProperties()...");

		Properties connectionProperties = new Properties();
		connectionProperties.setProperty("useSSL", "true");
		connectionProperties.setProperty("user", username);

		String password = System.getenv("password");
		Boolean useToken = Boolean.valueOf(System.getenv("useToken"));
		if (useToken)
			password = generateAuthToken(username, dbEndpoint, region, port);
		connectionProperties.setProperty("password", password);

		logger.info("Completed setConnectionProperties()");
		return connectionProperties;
	}
}
