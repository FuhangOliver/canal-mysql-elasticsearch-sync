package com.ciwei;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.annotation.EnableTransactionManagement;


/**
 * @author fuhang
 */
@SpringBootApplication
@EnableScheduling
@EnableTransactionManagement
public class App {
	private final static Logger logger = LoggerFactory.getLogger(App.class);

	public static void main(String[] args) {
		SpringApplication.run(App.class, args);
		logger.info("-----------canal-mysql-elasticsearch-sync服务开启成功------------");
	}
}
