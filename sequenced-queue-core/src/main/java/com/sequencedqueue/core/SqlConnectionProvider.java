package com.sequencedqueue.core;

import java.sql.Connection;

public interface SqlConnectionProvider {
    Connection currentConnection();
}
