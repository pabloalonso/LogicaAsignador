import com.bonitasoft.bbva.asignador.Asignador;
import org.bonitasoft.engine.api.ApiAccessType;
import org.bonitasoft.engine.util.APITypeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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
    public void before(){

            Map<String, String> settings = new HashMap<String, String>();
            settings.put("server.url", BONITA_URL);
            settings.put("application.name", BONITA_APP_NAME);
            APITypeManager.setAPITypeAndParams(ApiAccessType.HTTP, settings);

    }

    @Test
    public void nextTask() throws Exception {
        Long s = System.currentTimeMillis();
        Asignador as = Asignador.getAsignador(4L, "test", "install","install");
        Map<String, Serializable> task = as.getNextTask();
        LOGGER.info(task.toString());
        LOGGER.info("Tomo: " + (System.currentTimeMillis() - s) + "ms");
    }

    @Test(threadPoolSize = 10, invocationCount = 40, timeOut = 10000)
    public void mutiThreadNextTask() throws Exception {
        Long s = System.currentTimeMillis();
        Asignador as = Asignador.getAsignador(this.getNextUserId(), "test","install","install");
        Map<String, Serializable> task = as.getNextTask();
        LOGGER.info(task.toString());
        LOGGER.info("Tomo: " + (System.currentTimeMillis() - s) + "ms");

    }

    public static long getNextUserId(){
        Random randomGenerator = new Random();
        return randomGenerator.nextInt(10);
    }


}
