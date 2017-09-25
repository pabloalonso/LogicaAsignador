import com.bonitasoft.bbva.asignador.Asignador;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.Map;

/**
 * @author Pablo Alonso de Linaje García
 */
public class AsignadorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsignadorTest.class);

    @Test
    public void nextTask() {
        Long s = System.currentTimeMillis();
        Asignador as = Asignador.getAsignador(4L, "");
        Map<String, Serializable> task = as.getNextTask();
        LOGGER.error(task.toString());
        LOGGER.error("Tomo: " + (System.currentTimeMillis() - s) + "ms");
    }

    //@Test(threadPoolSize = 2, invocationCount = 20, timeOut = 10000)
    public void mutiThreadNextTask() {
        Long s = System.currentTimeMillis();
        Asignador as = Asignador.getAsignador(4L, "");
        Map<String, Serializable> task = as.getNextTask();
        LOGGER.error(task.toString());
        LOGGER.error("Tomo: " + (System.currentTimeMillis() - s) + "ms");

    }
}
