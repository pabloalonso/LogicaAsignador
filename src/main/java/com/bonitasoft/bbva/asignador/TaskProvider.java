package com.bonitasoft.bbva.asignador;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.logging.Logger;

/**
 * Created by pablo on 19/09/2017.
 */
public class TaskProvider {

    private static final Logger LOGGER = Logger.getLogger(TaskProvider.class.getName());
    private static final Object lock = new Object();
    private static volatile TaskProvider provider;
    private final Queue<Map<String, Object>> queueTasks = new LinkedList();
    private Long noResultsLastTry = 0L;
    private Long lastRefill = 0L;
    private DataSource dsBPM;

    private TaskProvider() {
    }

    private TaskProvider(String dataSourceBPM) {
        super();
        try {
            Context ctx = new InitialContext();
            dsBPM = (DataSource) ctx.lookup(dataSourceBPM);
            //getResultsFromDB();
        } catch (Exception e) {
            LOGGER.severe("Exception found trying to get the datasource with name " + dataSourceBPM + ": " + e.getMessage());


        }
    }

    public static TaskProvider getInstance(String dataSourceBPM) {
        TaskProvider r = provider;
        if (r == null) {
            synchronized (lock) {    // While we were waiting for the lock, another
                r = provider;        // thread may have instantiated the object.
                if (r == null) {
                    r = new TaskProvider(dataSourceBPM);
                    provider = r;
                }
            }
        }
        return r;
    }
}
