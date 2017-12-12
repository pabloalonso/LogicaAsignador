package com.bonitasoft.bbva.asignador.utils;


import com.bonitasoft.bbva.asignador.beans.Orden;
import com.bonitasoft.bbva.asignador.beans.Parametria;
import com.bonitasoft.bbva.asignador.beans.Prioridad;
import com.bonitasoft.bbva.asignador.beans.Restriccion;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Pablo Alonso de Linaje García
 */
public class ServicesAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServicesAccessor.class);

    public static Parametria getUserParametria(Long userId,String categoria, String serviceEndpoint) throws IOException {
    HttpClient client = new DefaultHttpClient();
//    String url = serviceEndpoint+"/reglas?usuario"+userId+"&categoria="+categoria;
    String url = serviceEndpoint+"/reglas/"+categoria+"/"+userId;

    HttpGet request = new HttpGet(url);
    HttpResponse response = client.execute(request);

    HttpEntity entity = response.getEntity();

        if (entity != null) {
            

        // A Simple JSON Response Read
        InputStream instream = entity.getContent();
        String result = convertStreamToString(instream);
        
        ObjectMapper mapper = new ObjectMapper();
        Map<String,Object> json = mapper.readValue(result, new TypeReference<Map<String,Object>>() {
        });
        
        instream.close();
        return buildParametria(json);
    }else{
        return null;
    }
}

    private static Parametria buildParametria(Map<String, Object> json) {
        LOGGER.error(json.toString());
        Parametria parametria = new Parametria();
        List<Restriccion> listaRestricciones = new ArrayList<Restriccion>();
        Object o = json.get("restricciones");
        Restriccion rest;
        if(o != null && o instanceof List){
            for(Object oR : (List) o){
                if(oR != null && oR instanceof Map) {
                    rest = new Restriccion();
                    rest.setRestriccion((String) ((Map) oR).get(new String("nombre")));
                    rest.setCondicion((String) ((Map) oR).get(new String("condicion")));
                    List valores = (List) ((Map) oR).get(new String("valor"));
                    String[] array = new String[valores.size()];
                    for (int i = 0; i < valores.size(); i++) {
                        array[i] = valores.get(i).toString();
                    }
                    rest.setValores(array);
                    listaRestricciones.add(rest);
                }
            }
        }

        o = json.get("prioridades");
        List<Prioridad> prioridades = new ArrayList<>();
        Prioridad prio;
        if(o != null && o instanceof List){
            for(Object oR : (List) o){
                if(oR != null && oR instanceof Map) {
                    prio = new Prioridad();
                    prio.setPrioridad((String) ((Map) oR).get(new String("nombre")));
                    prio.setCondicion((String) ((Map) oR).get(new String("condicion")));
                    List valores = (List) ((Map) oR).get(new String("valor"));
                    String[] array = new String[valores.size()];
                    for (int i = 0; i < valores.size(); i++) {
                        array[i] = valores.get(i).toString();
                    }
                    prio.setValor(array);
                    prio.setPeso(new Integer((String) ((Map) oR).get(new String("peso"))));
                    prioridades.add(prio);
                }
            }
        }

        o = json.get("ordenes");
        List<Orden> ordenes = new ArrayList<>();
        Orden orden = new Orden();
        if(o != null && o instanceof List){
            for(Object oR : (List) o){
                if(oR != null && oR instanceof Map) {
                    orden = new Orden();
                    orden.setCriterio((String) ((Map) oR).get(new String("criterio")));
                    orden.setOrden((String) ((Map) oR).get(new String("nombre")));
                    ordenes.add(orden);
                }
            }
        }

        parametria.setOrdenList(ordenes);
        parametria.setPrioridadList(prioridades);
        parametria.setRestriccionList(listaRestricciones);

        return parametria;
    }

    private static String convertStreamToString(InputStream is) {

        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        StringBuilder sb = new StringBuilder();

        String line = null;
        try {
            while ((line = reader.readLine()) != null) {
                sb.append(line + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return sb.toString();
    }

}
