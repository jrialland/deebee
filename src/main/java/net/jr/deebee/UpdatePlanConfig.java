package net.jr.deebee;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;

public class UpdatePlanConfig {

    public static final String CONNECTION_PROVIDER = "connectionProvider";

    public static final String USER = "user";

    private Map<String, Object> config = new TreeMap<>();

    public <T> T get(String key) {
        return (T) config.get(key);
    }

    public <T> UpdatePlanConfig set(String key, T value) {
        config.put(key, value);
        return this;
    }

    public UpdatePlanConfig setConnectionProvider(Callable<Connection> connectionProvider) {
        set(CONNECTION_PROVIDER, connectionProvider);
        return this;
    }

    public UpdatePlanConfig setDataSource(DataSource dataSource) {
        set("dataSource", dataSource);
        setConnectionProvider(() -> dataSource.getConnection());
        return this;
    }

    public UpdatePlanConfig setConnection(Connection connection) {
        set("connection", connection);
        setConnectionProvider(() -> connection);
        return this;
    }

    public UpdatePlanConfig setUser(String user) {
        set(USER, user);
        return this;
    }
}
