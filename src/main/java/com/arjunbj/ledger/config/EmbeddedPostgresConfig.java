package com.arjunbj.ledger.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.nio.file.Path;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Starts a real PostgreSQL server in-process (zonky binaries from Maven) when no
 * spring.datasource.url is configured, so tests, the benchmark and a local run need no install.
 * Setting SPRING_DATASOURCE_URL switches this off and Spring Boot's normal DataSource takes over.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnExpression("'${spring.datasource.url:}'.isEmpty()")
public class EmbeddedPostgresConfig {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedPostgresConfig.class);

    @Bean(destroyMethod = "close")
    EmbeddedPostgres embeddedPostgres(@Value("${ledger.db.embedded-data-dir:}") String dataDir,
                                      @Value("${ledger.db.embedded-port:0}") int port) throws IOException {
        EmbeddedPostgres.Builder builder = EmbeddedPostgres.builder();
        if (!dataDir.isBlank()) {
            // Keep data between runs: zonky only runs initdb when the directory has no cluster yet.
            builder.setDataDirectory(Path.of(dataDir)).setCleanDataDirectory(false);
        }
        if (port > 0) {
            builder.setPort(port);
        }
        EmbeddedPostgres postgres = builder.start();
        log.info("embedded PostgreSQL listening on port {}", postgres.getPort());
        return postgres;
    }

    @Bean
    DataSource dataSource(EmbeddedPostgres postgres, @Value("${ledger.db.pool-size:20}") int poolSize) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgres.getJdbcUrl("postgres", "postgres"));
        config.setMaximumPoolSize(poolSize);
        config.setPoolName("ledger");
        return new HikariDataSource(config);
    }
}
