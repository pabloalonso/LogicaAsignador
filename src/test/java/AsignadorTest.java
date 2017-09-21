import com.bonitasoft.bbva.asignador.Asignador;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by pablo on 20/09/2017.
 */
public class AsignadorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsignadorTest.class);

    public void nextTask() {
        Asignador as = Asignador.getAsignador(4L, "");
        as.getNextTask();
    }

    //@Test(threadPoolSize = 30, invocationCount = 60, timeOut = 10000)
    @Test
    public void mutiThreadNextTask() {
        Long s = System.currentTimeMillis();
        Asignador as = Asignador.getAsignador(4L, "");
        Map<String, Serializable> task = as.getNextTask();
        LOGGER.error(task.toString());
        LOGGER.error("Tomo: " + (System.currentTimeMillis() - s) + "ms");

    }
}
