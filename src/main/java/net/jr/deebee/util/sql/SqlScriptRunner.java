package net.jr.deebee.util.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.CharArrayWriter;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;

public class SqlScriptRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlScriptRunner.class);

    private Callable<Connection> connectionProvider;

    public SqlScriptRunner(Connection connection) {
        this(() -> connection);
    }

    public SqlScriptRunner(DataSource dataSource) {
        this(() -> dataSource.getConnection());
    }

    public SqlScriptRunner(Callable<Connection> callable) {
        this.connectionProvider = callable;
    }

    private Connection getConnection() throws Exception {
        return connectionProvider.call();
    }

    public interface TransactionCallback {
        void doInTransaction(Connection connection) throws SQLException;
    }

    private void doInTransaction(TransactionCallback fn) throws Exception {
        Connection cnx = getConnection();
        boolean autoCommit = cnx.getAutoCommit();
        try {
            if (autoCommit) {
                cnx.setAutoCommit(false);
            }
            fn.doInTransaction(cnx);
            cnx.commit();
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException(e);
        } finally {
            cnx.rollback();
            if (autoCommit) {
                cnx.setAutoCommit(true);
            }
        }
    }

    public void run(String scriptName, Reader script) {

        try {

            LOGGER.info("Applying " + scriptName+" ...");

            //read script
            CharArrayWriter cw = new CharArrayWriter();
            char[] buffer = new char[512];
            int r;
            while ((r = script.read(buffer)) > -1) {
                cw.write(buffer, 0, r);
            }
            script.close();

            //run all statements in a transaction
            doInTransaction((connection) -> {
                for (String cmd : cw.toString().split(";")) {
                    connection.createStatement().execute(cmd.trim());
                }
            });

            LOGGER.info(" ... Done");

        } catch (Exception e) {
            LOGGER.error("While applying " + scriptName, e);
            throw new RuntimeException(e);
        }
    }
}
