package com.arjunbj.ledger.recon;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** The three ways to run the reconciler: HTTP endpoint, schedule, and one-shot CLI. */
public final class ReconcilerEntryPoints {

    private ReconcilerEntryPoints() {
    }

    @RestController
    static class Endpoint {

        private final Reconciler reconciler;

        Endpoint(Reconciler reconciler) {
            this.reconciler = reconciler;
        }

        @GetMapping("/reconciliation")
        Reconciler.Report run() {
            return reconciler.run();
        }
    }

    @Component
    @ConditionalOnProperty(name = "ledger.reconciler.scheduled", havingValue = "true", matchIfMissing = true)
    static class Job {

        private static final Logger log = LoggerFactory.getLogger(Job.class);
        private final Reconciler reconciler;

        Job(Reconciler reconciler) {
            this.reconciler = reconciler;
        }

        @Scheduled(fixedDelayString = "${ledger.reconciler.interval-ms:60000}", initialDelayString = "${ledger.reconciler.interval-ms:60000}")
        void run() {
            Reconciler.Report report = reconciler.run();
            if (report.ok()) {
                log.info("reconciliation ok: {} transfers, {} entries, read model: {}",
                        report.transfers(), report.entries(), report.readModelNote());
            } else {
                log.error("reconciliation found {} violations: {}", report.violations().size(), report.violations());
            }
        }
    }

    /** java -jar app.jar --spring.main.web-application-type=none --ledger.cli=reconcile ; exit code 1 on violations. */
    @Component
    @ConditionalOnProperty(name = "ledger.cli", havingValue = "reconcile")
    static class Cli implements ApplicationRunner {

        private final Reconciler reconciler;
        private final ObjectMapper json;
        private final ConfigurableApplicationContext context;

        Cli(Reconciler reconciler, ObjectMapper json, ConfigurableApplicationContext context) {
            this.reconciler = reconciler;
            this.json = json;
            this.context = context;
        }

        @Override
        public void run(ApplicationArguments args) throws Exception {
            Reconciler.Report report = reconciler.run();
            System.out.println(json.writerWithDefaultPrettyPrinter().writeValueAsString(report));
            int code = report.ok() ? 0 : 1;
            System.exit(SpringApplication.exit(context, () -> code));
        }
    }
}
