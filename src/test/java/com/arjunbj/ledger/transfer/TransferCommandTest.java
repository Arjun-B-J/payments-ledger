package com.arjunbj.ledger.transfer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class TransferCommandTest {

    private final TransferCommand base = new TransferCommand(1, 2, 500, "USD", false);

    @Test
    void sameFieldsGiveTheSameHash() {
        assertThat(new TransferCommand(1, 2, 500, "USD", false).requestHash()).isEqualTo(base.requestHash());
        assertThat(base.requestHash()).hasSize(64);
    }

    @Test
    void everyFieldChangesTheHash() {
        List<TransferCommand> variants = List.of(
                new TransferCommand(9, 2, 500, "USD", false),
                new TransferCommand(1, 9, 500, "USD", false),
                new TransferCommand(1, 2, 501, "USD", false),
                new TransferCommand(1, 2, 500, "EUR", false),
                new TransferCommand(1, 2, 500, "USD", true));
        assertThat(variants).allSatisfy(v -> assertThat(v.requestHash()).isNotEqualTo(base.requestHash()));
    }
}
