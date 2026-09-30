package com.arjunbj.ledger.common;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

/**
 * Named fault-injection points. Tests arm a point to throw at an exact place (between the two
 * balance legs, after commit, between delivery and the published mark) to prove what a crash there
 * leaves behind. Nothing is armed in normal runs, so a hit costs one empty-map check.
 */
@Component
public class FailPoints {

    public enum Point { AFTER_DEBIT_LEG, AFTER_COMMIT, RELAY_AFTER_DELIVER }

    public static class InjectedFault extends RuntimeException {
        public InjectedFault(Point point) {
            super("injected fault at " + point);
        }
    }

    private final Map<Point, AtomicInteger> armed = new ConcurrentHashMap<>();

    public void arm(Point point, int times) {
        armed.put(point, new AtomicInteger(times));
    }

    public void disarmAll() {
        armed.clear();
    }

    public void hit(Point point) {
        if (armed.isEmpty()) {
            return;
        }
        AtomicInteger remaining = armed.get(point);
        if (remaining != null && remaining.getAndDecrement() > 0) {
            throw new InjectedFault(point);
        }
    }
}
