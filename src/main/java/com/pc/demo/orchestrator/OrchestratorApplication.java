package com.pc.demo.orchestrator;

import java.util.function.Function;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class OrchestratorApplication {

	public static void main(String[] args) {
		//SpringApplication.run(OrchestratorApplication.class, args);
	}
	
	@Bean
	public Function<String, String> uppercase() {
		return value -> {
			if (value.equals("exception")) {
				throw new RuntimeException("Intentional exception");
			}
			else {
				System.out.println("value.toUpperCase() -> " + value.toUpperCase());
				return value.toUpperCase();
			}
		};
	}
}
