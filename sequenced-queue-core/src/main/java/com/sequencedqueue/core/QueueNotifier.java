package com.sequencedqueue.core;

import java.sql.Connection;
import java.sql.SQLException;

public interface QueueNotifier {
    void notifyWorkAvailable(Connection connection, QueueWakeupEvent event) throws SQLException;
}
