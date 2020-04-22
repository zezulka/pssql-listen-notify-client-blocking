package cz.fi.muni.pa036.listennotify.client.blocking;

import cz.fi.muni.pa036.listennotify.api.AbstractListenNotifyClient;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.postgresql.ds.PGSimpleDataSource;

/**
 *
 * @author Miloslav Zezulka
 */
public class ListenNotifyClientBlocking extends AbstractListenNotifyClient {
    
    private PGConnection pgConn;
    private Connection conn;
    private BlockingQueue<String> queue = new ArrayBlockingQueue<>(10);
    
    public ListenNotifyClientBlocking() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerNames(new String[]{"localhost"});
        ds.setDatabaseName("postgres");
        ds.setPortNumbers(new int[]{5432});
        ds.setUser("postgres");
        ds.setPassword("");
        try {
            conn = ds.getConnection();
            pgConn = conn.unwrap(org.postgresql.PGConnection.class);
            new NotificationPoller(pgConn).start();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
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
    protected String nextRawJson() {
        try {
            return queue.take();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    class NotificationPoller extends Thread {
        
        private final PGConnection conn;
        private final PGNotification[] EMPTY_NOTIFS = new PGNotification[]{};
        
        public NotificationPoller(PGConnection conn) {
            Objects.requireNonNull(conn);
            this.conn = conn;
        }
        
        @Override
        public void run() {
            while (true) {
                try {
                    PGNotification[] notifs = conn.getNotifications();
                    for (PGNotification notification : (notifs == null ? EMPTY_NOTIFS : notifs)) {
                        queue.add(notification.getParameter());
                    }
                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
        
    }
}
