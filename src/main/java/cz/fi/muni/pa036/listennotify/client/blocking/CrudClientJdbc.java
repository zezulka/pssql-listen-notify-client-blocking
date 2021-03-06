package cz.fi.muni.pa036.listennotify.client.blocking;

import cz.fi.muni.pa036.listennotify.api.CrudClient;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * Implementation of the CrudClient using standard low-level JDBC API.
 * @author Miloslav Zezulka
 */
public class CrudClientJdbc extends CrudClient {

    private PGConnection pgConn;
    private Connection conn;
    
    public CrudClientJdbc() {
        Logger.getGlobal().info(Thread.currentThread().getName() + " just created a new DS");
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerNames(new String[]{"localhost"});
        ds.setDatabaseName("postgres");
        ds.setPortNumbers(new int[]{5432});
        ds.setUser("postgres");
        ds.setPassword("");
        try {
            conn = ds.getConnection();
            pgConn = conn.unwrap(org.postgresql.PGConnection.class);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    @Override
    protected Connection getConnection() {
        return conn;
    }
    
    @Override
    protected Statement createStatement() {
        try {
            return conn.createStatement();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    @Override
    protected PreparedStatement createPreparedStatement(String string) {
        Logger.getGlobal().info(String.format("Creating prepared statement for the query '%s'...", string));
        try {
            return conn.prepareStatement(string);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    PGNotification[] getNotifications() throws SQLException {
        return pgConn.getNotifications();
    }
}
