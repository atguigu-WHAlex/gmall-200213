package com.atguigu.gmallpulisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.gmallpulisher.mapper")
public class GmallPulisherApplication {

	public static void main(String[] args) {
		SpringApplication.run(GmallPulisherApplication.class, args);
	}

}