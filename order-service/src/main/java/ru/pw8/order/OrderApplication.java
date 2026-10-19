package ru.pw8.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import ru.pw8.common.Infrastructure;

@SpringBootApplication
@Import(Infrastructure.class)
public class OrderApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderApplication.class, args);
    }

    @Bean
    TransactionTemplate transactions(PlatformTransactionManager manager) {
        return new TransactionTemplate(manager);
    }
}
