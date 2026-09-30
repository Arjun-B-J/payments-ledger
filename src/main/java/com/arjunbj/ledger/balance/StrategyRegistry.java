package com.arjunbj.ledger.balance;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Holds the strategy in use. Set by ledger.strategy; tests and the benchmark switch it per run. */
@Component
public class StrategyRegistry {

    private final Map<StrategyName, BalanceStrategy> strategies = new EnumMap<>(StrategyName.class);
    private volatile StrategyName active;

    public StrategyRegistry(List<BalanceStrategy> all, @Value("${ledger.strategy:ATOMIC}") StrategyName initial) {
        all.forEach(s -> strategies.put(s.name(), s));
        use(initial);
    }

    public BalanceStrategy active() {
        return strategies.get(active);
    }

    public void use(StrategyName name) {
        if (!strategies.containsKey(name)) {
            throw new IllegalArgumentException("unknown strategy " + name);
        }
        active = name;
    }
}
