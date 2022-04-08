package com.pc.demo.orchestrator;

import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;

import com.pc.demo.orchestrator.service.DBService;

@SpringBootApplication
public class OrchestratorApplication {

	private static final Logger logger = LogManager.getLogger(OrchestratorApplication.class);

	public static void main(String[] args) {
		new SpringApplicationBuilder(OrchestratorApplication.class).logStartupInfo(false).run(args);
	}

	@Autowired
	DBService dbService;

	@Bean
	public Function<String, String> uppercase() {
		return value -> {
			logger.info("Inside Lambda Handler...");

			try {
				dbService.getData();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			if (value.equals("exception")) {
				throw new RuntimeException("Intentional exception");
			} else {
				System.out.println("value.toUpperCase() -> " + value.toUpperCase());
				return value.toUpperCase();
			}
		};
	}
}
