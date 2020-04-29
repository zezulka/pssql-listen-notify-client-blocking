package cz.fi.muni.pa036.listennotify.client.blocking;

import cz.fi.muni.pa036.listennotify.api.AbstractListenNotifyClient;
import cz.fi.muni.pa036.listennotify.api.event.EventType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;
import org.postgresql.PGNotification;

/**
 *
 * @author Miloslav Zezulka
 */
public class ListenNotifyClientBlocking extends AbstractListenNotifyClient {

    private final BlockingQueue<String> textQueue = new ArrayBlockingQueue<>(1024*1024);
    private final BlockingQueue<String> binaryQueue = new ArrayBlockingQueue<>(1024*1024);
    
    @Override
    public void run() {
        Logger.getGlobal().info("Started listen-notify client.");
        if(crudClient == null) {
            throw new IllegalStateException("You must set CRUD client first before running this thread.");
        }
        while (true) {
            try {
                PGNotification[] notifs = ((CrudClientJdbc)crudClient).getNotifications();
                if (notifs != null) {
                    Logger.getGlobal().info(Thread.currentThread().getName() + ": got " + notifs.length + " notifs");
                    for(PGNotification notif : notifs) {
                        getQueue(ChannelName.valueOf(notif.getName().toUpperCase()))
                                .add(notif.getParameter());
                    }
                }
                if (Thread.currentThread().isInterrupted()) {
                    textQueue.clear();
                    binaryQueue.clear();
                    return;
                 }
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private BlockingQueue<String> getQueue(ChannelName tn) {
        switch(tn) {
            case Q_EVENT_BIN: return binaryQueue;
            case Q_EVENT: return textQueue;
            default: throw new IllegalArgumentException(tn + " channel not supported");
        }
    }
    
    @Override
    protected String nextRawJson(ChannelName tn) {
        BlockingQueue<String> queue = getQueue(tn);
        try {
            return queue.take();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    protected List<String> nextRawJson(ChannelName tn, int noElements) {
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
