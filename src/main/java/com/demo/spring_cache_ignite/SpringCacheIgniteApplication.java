package com.demo.spring_cache_ignite;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

@EnableWebMvc
@SpringBootApplication
public class SpringCacheIgniteApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringCacheIgniteApplication.class, args);
	}
}
