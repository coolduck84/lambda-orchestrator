package com.pc.demo.orchestrator.config;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsUtilities;
import software.amazon.awssdk.services.rds.model.GenerateAuthenticationTokenRequest;

@Configuration
@EnableJpaRepositories(basePackages = "com.pc.demo.orchestrator.repository")
public class JpaConfig {

	private static final String JDBC_PREFIX = "jdbc:postgresql://";
	private static final Logger logger = LogManager.getLogger(JpaConfig.class);

	private static final Integer DB_PORT = Integer.valueOf(System.getenv("port"));
	private static final String DB_ENDPOINT = System.getenv("dbEndpoint");
	private static final String DB_REGION = System.getenv("region");
	private static final String DB_USER = System.getenv("userName");
	private static final String DB_SCHEMA = System.getenv("schema");
	private static final String DB_DATABASE = System.getenv("database");

	@Bean
	public DataSource dataSource() {
		logger.info("Inside dataSource()...");

		logger.info("==== Environment Variables ====");
		logger.info("DB_PORT: " + DB_PORT);
		logger.info("DB_ENDPOINT: " + DB_ENDPOINT);
		logger.info("DB_REGION: " + DB_REGION);
		logger.info("DB_USER: " + DB_USER);
		logger.info("DB_SCHEMA: " + DB_SCHEMA);
		logger.info("==== Environment Variables ====");

		String password = generateAuthToken(DB_USER, DB_ENDPOINT, DB_REGION, DB_PORT);
		logger.info("password: " + password);

		DataSource dataSource = DataSourceBuilder.create().username(DB_USER).password(password)
				.url(JDBC_PREFIX + DB_ENDPOINT + "/" + DB_DATABASE).driverClassName("org.postgresql.Driver").build();

		logger.info("Completed dataSource() !");

		return dataSource;
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
}