import com.bonitasoft.bbva.asignador.Asignador;
import org.bonitasoft.engine.api.ApiAccessType;
import org.bonitasoft.engine.util.APITypeManager;
import org.h2.jdbcx.JdbcConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @author Pablo Alonso de Linaje García
 */
public class AsignadorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsignadorTest.class);
    private static final String BONITA_URL = "http://localhost:8080";
    private static final String BONITA_APP_NAME = "bonita";

    @BeforeClass
    public void before() throws Exception {

            Map<String, String> settings = new HashMap<String, String>();
            settings.put("server.url", BONITA_URL);
            settings.put("application.name", BONITA_APP_NAME);
            APITypeManager.setAPITypeAndParams(ApiAccessType.HTTP, settings);
            createDatasource();
    }
    private void createDatasource() throws Exception {
        // rcarver - setup the jndi context and the datasource
        try {
            // Create initial context
            System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.apache.naming.java.javaURLContextFactory");
            System.setProperty(Context.URL_PKG_PREFIXES, "org.apache.naming");
            InitialContext ic = new InitialContext();

            ic.createSubcontext("java:");
            ic.createSubcontext("java:/comp");
            ic.createSubcontext("java:/comp/env");
            ic.createSubcontext("java:/comp/env/jdbc");
            JdbcConnectionPool ds = JdbcConnectionPool.create(
                    "jdbc:h2:file:C:\\BonitaBPM\\workspace\\BBVA-AsignadorTareas\\h2_database//business_data.db;MVCC=TRUE;DB_CLOSE_ON_EXIT=FALSE;IGNORECASE=TRUE;AUTO_SERVER=TRUE;", "sa", "");
            ic.bind("java:/test", ds);
        } catch (NamingException ex) {
            LOGGER.error(null, ex);
        }

    }

    @Test
    public void nextTask() throws Exception {
        Long s = System.currentTimeMillis();
        Asignador as = Asignador.getAsignador(4L, "java:/test", "install","install", "categoria");
        Map<String, Serializable> task = as.getNextTask();
        LOGGER.info(task.toString());
        LOGGER.info("Tomo: " + (System.currentTimeMillis() - s) + "ms");
    }

    //@Test(threadPoolSize = 10, invocationCount = 40, timeOut = 10000)
    public void mutiThreadNextTask() throws Exception {
        Long s = System.currentTimeMillis();
        Asignador as = Asignador.getAsignador(this.getNextUserId(), "java:/test","install","install", "test");
        Map<String, Serializable> task = as.getNextTask();
        LOGGER.info(task.toString());
        LOGGER.info("Tomo: " + (System.currentTimeMillis() - s) + "ms");

    }

    public static long getNextUserId(){
        Random randomGenerator = new Random();
        return randomGenerator.nextInt(10);
    }


}
