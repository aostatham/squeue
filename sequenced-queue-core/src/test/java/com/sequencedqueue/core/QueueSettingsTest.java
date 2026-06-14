package com.sequencedqueue.core;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class QueueSettingsTest {
    @Test
    void rejectsInvalidGlobalConfiguration() {
        assertThrows(IllegalArgumentException.class, () -> new QueueSettings(0, 600, 5, 100, 100, 100, 100));
        assertThrows(IllegalArgumentException.class, () -> new QueueSettings(60, 0, 5, 100, 100, 100, 100));
        assertThrows(IllegalArgumentException.class, () -> new QueueSettings(601, 600, 5, 100, 100, 100, 100));
        assertThrows(IllegalArgumentException.class, () -> new QueueSettings(60, 600, 0, 100, 100, 100, 100));
        assertThrows(IllegalArgumentException.class, () -> new QueueSettings(60, 600, 5, 0, 100, 100, 100));
        assertThrows(IllegalArgumentException.class, () -> new QueueSettings(60, 600, 5, 100, 0, 100, 100));
        assertThrows(IllegalArgumentException.class, () -> new QueueSettings(60, 600, 5, 100, 100, 0, 100));
        assertThrows(IllegalArgumentException.class, () -> new QueueSettings(60, 600, 5, 100, 100, 100, 0));
    }
}
