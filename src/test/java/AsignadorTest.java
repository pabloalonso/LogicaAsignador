import com.bonitasoft.bbva.asignador.Asignador;
import oracle.jdbc.pool.OracleDataSource;
import org.bonitasoft.engine.api.ApiAccessType;
import org.bonitasoft.engine.util.APITypeManager;
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

//import org.h2.jdbcx.JdbcConnectionPool;

//import org.h2.jdbcx.JdbcConnectionPool;

/**
 * @author Pablo Alonso de Linaje García
 */
public class AsignadorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsignadorTest.class);
    //private static final String BONITA_URL = "http://localhost:8080";
    private static final String BONITA_URL = "http://192.168.4.90:8080";
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

            //java:jboss/datasources/bdmAsignador

            ic.createSubcontext("java:");
            ic.createSubcontext("java:jboss");
            ic.createSubcontext("java:jboss/datasources");
            //ic.createSubcontext("java:jboss/datasources");

            OracleDataSource ds = new OracleDataSource();
            ds.setURL("jdbc:oracle:thin:@192.168.4.251:1521:xe");
            ds.setUser("bonitaBDM");
            ds.setPassword("bonita");
            /*
            JdbcConnectionPool ds = JdbcConnectionPool.create(
                    "jdbc:h2:file:C:\\BonitaBPM\\workspace\\BBVA-AsignadorTareas\\h2_database//business_data.db;MVCC=TRUE;DB_CLOSE_ON_EXIT=FALSE;IGNORECASE=TRUE;AUTO_SERVER=TRUE;", "sa", "");
                    */
            ic.bind("java:jboss/datasources/bdmAsignador", ds);
        } catch (NamingException ex) {
            LOGGER.error(null, ex);
        }

    }

    //@Test
    public void nextTask() throws Exception {
        Long s = System.currentTimeMillis();
        Asignador as = Asignador.getAsignador(102L, "java:jboss/datasources/bdmAsignador", "install","install", "Natural", "http://192.168.10.8:8088/services");
        Map<String, Serializable> task = as.getNextTask();
        LOGGER.info(task.toString());
        LOGGER.info("Tomo: " + (System.currentTimeMillis() - s) + "ms");
    }

    @Test(threadPoolSize = 5, invocationCount = 30, timeOut = 10000)
    public void mutiThreadNextTask() throws Exception {
        Long s = System.currentTimeMillis();
        Long userId = this.getNextUserId();
        Asignador as = Asignador.getAsignador(userId, "java:jboss/datasources/bdmAsignador","install","install", "Natural","http://192.168.10.8:8088/services");
        Map<String, Serializable> task = as.getNextTask();
        if(task == null) {
            LOGGER.info("No hay tareas para "+userId);
        }else{
            LOGGER.info(task.toString());
        }
        LOGGER.info("Tomo: " + (System.currentTimeMillis() - s) + "ms");

    }

    public static long getNextUserId(){
        Random randomGenerator = new Random();
        return 100+randomGenerator.nextInt(10);
    }


}
