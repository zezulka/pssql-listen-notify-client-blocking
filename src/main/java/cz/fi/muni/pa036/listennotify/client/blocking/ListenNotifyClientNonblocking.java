package cz.fi.muni.pa036.listennotify.client.blocking;

import cz.fi.muni.pa036.listennotify.api.AbstractListenNotifyClient;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.postgresql.ds.PGSimpleDataSource;

/**
 *
 * @author Miloslav Zezulka
 */
public class ListenNotifyClientNonblocking extends AbstractListenNotifyClient {
    
    private PGConnection pgConn;
    private Connection conn;
    private BlockingQueue<String> queue = new ArrayBlockingQueue<>(10);
    
    public ListenNotifyClientNonblocking() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerNames(new String[]{"localhost"});
        ds.setDatabaseName("postgres");
        ds.setPortNumbers(new int[]{5432});
        ds.setUser("postgres");
        ds.setPassword("");
        try {
            pgConn = ds.getConnection().unwrap(org.postgresql.PGConnection.class);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        new NotificationPoller(pgConn).start();
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
        
        private PGConnection conn;
        
        public NotificationPoller(PGConnection conn) {
            this.conn = conn;
        }
        
        @Override
        public void run() {
            while (true) {
                try {
                    for (PGNotification notification : conn.getNotifications()) {
                        queue.add(notification.getParameter());
                    }
                    Thread.sleep(500);
                } catch (SQLException | InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
        
    }
}
