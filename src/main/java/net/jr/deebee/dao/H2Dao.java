package net.jr.deebee.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.jr.deebee.UpdatePlanBuilder;
import net.jr.deebee.model.DbUpdateLog;
import net.jr.deebee.model.DbUpdateStatus;
import net.jr.deebee.util.sql.NamedPreparedStatement;

import java.sql.*;
import java.util.Map;
import java.util.concurrent.Callable;

public class H2Dao implements Dao {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private Callable<Connection> connectionProvider;

    private ThreadLocal<Connection> connection = new ThreadLocal<>();

    private ThreadLocal<Boolean>  originalAutocommit = new ThreadLocal<>();

    public H2Dao(Connection connection) {
        connectionProvider = () -> connection;
    }

    @Override
    public String getCurrentVersionFromDb() throws Exception {
        Connection c = connection.get();
        ResultSet rs = c.createStatement().executeQuery("select to_version from db_update_status where current <> 0");
        if(rs.next()) {
            return rs.getString(1);
        } else {
            return UpdatePlanBuilder.INITIAL_VERSION;
        }
    }

    @Override
    public void insertLog(DbUpdateLog updateLog)  throws Exception {

        NamedPreparedStatement pStmt = NamedPreparedStatement.create(connection.get(), "insert into db_update_log(level, comment, tstamp) values(:level, :comment, current_timestamp)", Statement.RETURN_GENERATED_KEYS);
        pStmt.setParameters(objectMapper.convertValue(updateLog, Map.class));
        pStmt.executeUpdate();
        updateLog.setId(pStmt.getUniqueGeneratedKey());
    }

    @Override
    public void insertStatus(DbUpdateStatus updateStatus)  throws Exception {
        NamedPreparedStatement pStmt = NamedPreparedStatement.create(connection.get(),"insert into db_update_status(from_version, to_version, since, user, reference, comment) values(:fromVersion, :toVersion, current_timestamp, :user, :reference, :comment)");
        pStmt.setParameters(objectMapper.convertValue(updateStatus, Map.class));
        pStmt.executeUpdate();

        ResultSet rs = connection.get().createStatement().executeQuery("select max(id) from db_update_status");
        rs.next();
        long id = rs.getLong(1);
        updateStatus.setId(id);
    }

    @Override
    public void markAsCurrent(DbUpdateStatus updateStatus)  throws Exception {
        connection.get().createStatement().executeUpdate("update db_update_status set current = 0");
        NamedPreparedStatement
                .create(connection.get(), "update db_update_status set current = 1 where id = :id")
                .setLong("id", updateStatus.getId())
                .executeUpdate();
    }

    @Override
    public void begin()  throws Exception{
        connection.set(connectionProvider.call());
        originalAutocommit.set(connection.get().getAutoCommit());
        connection.get().setAutoCommit(false);
    }

    @Override
    public void commit()  throws Exception{
        connection.get().commit();
        connection.get().setAutoCommit(originalAutocommit.get());
    }

    @Override
    public void rollback() {
        try {
            connection.get().rollback();
            connection.get().setAutoCommit(originalAutocommit.get());
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void end() throws Exception {
        Connection c = connection.get();
        if(c != null && !c.isClosed()) {
            c.close();
        }
    }

    @Override
    public void ensureTablesExist() throws Exception {
        Connection c = connection.get();
        boolean hasDbUpdateTable = c.getMetaData().getTables(null, null, null,new String[]{"db_update_status"}).next();
        if(!hasDbUpdateTable) {
            //create missing tables
            c.createStatement().execute("create table db_update_status(id integer auto_increment, from_version char(32) not null, to_version char(32) not null, since timestamp not null, current integer, user char(256), reference char(256), comment char(256), primary key(id))");
            c.createStatement().execute("create table db_update_log(id integer auto_increment, level char(16) not null, comment char(1024), tstamp timestamp not null default current_timestamp, primary key(id))");
        }
    }
}
