import com.bonitasoft.bbva.asignador.beans.Parametria;
import com.bonitasoft.bbva.asignador.utils.ServicesAccessor;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * @author Pablo Alonso de Linaje García
 */
public class ServiceAccessorTest {

    @Test
    public void testServiceAccesor() throws IOException {
        Parametria p = ServicesAccessor.getUserParametria(36L, "Natural","http://192.168.10.8:8088/services");

        if(p!= null){
            System.out.println(p.toString());
        }


    }

}
