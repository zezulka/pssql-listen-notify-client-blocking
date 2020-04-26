package cz.fi.muni.pa036.listennotify.client.blocking;

import cz.fi.muni.pa036.listennotify.api.AbstractListenNotifyClient;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.postgresql.PGNotification;

/**
 *
 * @author Miloslav Zezulka
 */
public class ListenNotifyClientBlocking extends AbstractListenNotifyClient {

    private final BlockingQueue<String> textQueue = new ArrayBlockingQueue<>(1024);
    private final BlockingQueue<String> binaryQueue = new ArrayBlockingQueue<>(1024);
    // Loosely coupled but we have no other option here (we want to call getNotifications)
    private final CrudClientJdbc crudClient = new CrudClientJdbc();

    @Override
    public void run() {
        while (true) {
            try {
                PGNotification[] notifs = crudClient.getNotifications();
                if (notifs != null) {
                    for(PGNotification notif : notifs) {
                        getQueue(TableName.valueOf(notif.getName())).add(notif.getParameter());
                    }
                }
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private BlockingQueue<String> getQueue(TableName tn) {
        switch(tn) {
            case BIN: return binaryQueue;
            case TEXT: return textQueue;
            default: throw new IllegalArgumentException(tn + " table not supported");
        }
    }
    
    @Override
    protected String nextRawJson(TableName tn) {
        BlockingQueue<String> queue = getQueue(tn);
        try {
            return queue.take();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    protected List<String> nextRawJson(TableName tn, int noElements) {
        BlockingQueue<String> queue = getQueue(tn);
        if (queue.size() < noElements) {
            throw new IllegalArgumentException(
                    String.format("Cannot drain event queue by the number of %d "
                            + "since it only contains %d elements.", noElements, queue.size()));
        }
        List<String> result = new ArrayList<>(noElements);
        queue.drainTo(result, noElements);
        return result;
    }
}
