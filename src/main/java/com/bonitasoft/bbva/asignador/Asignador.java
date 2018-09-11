package com.bonitasoft.bbva.asignador;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import com.bonitasoft.bbva.asignador.beans.Orden;
import com.bonitasoft.bbva.asignador.beans.Parametria;
import com.bonitasoft.bbva.asignador.beans.Prioridad;
import com.bonitasoft.bbva.asignador.beans.Restriccion;
import com.bonitasoft.bbva.asignador.utils.IndexesASCComparator;
import com.bonitasoft.bbva.asignador.utils.IndexesDESCComparator;
import com.bonitasoft.bbva.asignador.utils.ServicesAccessor;
import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.api.TenantAPIAccessor;
import org.bonitasoft.engine.bpm.flownode.HumanTaskInstance;
import org.bonitasoft.engine.bpm.flownode.HumanTaskInstanceSearchDescriptor;
import org.bonitasoft.engine.exception.SearchException;
import org.bonitasoft.engine.exception.UpdateException;
import org.bonitasoft.engine.search.Order;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.session.APISession;
import org.bonitasoft.engine.session.InvalidSessionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Pablo Alonso de Linaje Garcia
 */
public class Asignador {
    private static String BONITA_TECH_USER;
    private static String BONITA_TECH_USER_PASSWORD;
    private static String REST_ENDPOINT;
    private static final Logger LOGGER = LoggerFactory.getLogger(Asignador.class);
    private static Map<String, Asignador> asignadorUsuario = new HashMap<>();
    private static DataSource dsBDM = null;
    private String categoria;
    private Long idUsuario;
    private static APISession session;

    private Asignador(Long idUsuario , String categoria) {
        super();
        this.idUsuario = idUsuario;
        this.categoria = categoria;


    }

    public static Asignador getAsignador(Long idUser, String datasourceBDM, String techUser, String techUserPwd, String categoria, String restEndpoint) throws Exception {
        BONITA_TECH_USER = techUser;
        BONITA_TECH_USER_PASSWORD = techUserPwd;
        REST_ENDPOINT = restEndpoint;
        try {
            if (dsBDM == null) {
                dsBDM = getDatasouce(datasourceBDM);
            }
        }catch (NamingException ne){
            throw new Exception("Conexion no conseguida",ne);
        }
        Asignador asignador = asignadorUsuario.get(""+idUser+"-"+categoria);
        if (asignador == null) {
            asignador = new Asignador(idUser, categoria);
            asignadorUsuario.put(""+idUser+"-"+categoria, asignador);
        }

        return asignador;
    }


    public Map<String, Serializable> getNextTask() throws Exception{

        Map<String, Serializable> task;

        Parametria params = getParametria(categoria);

        final List<Long> casosPrioritarios = getCasosPrioritarios(params);

        List<HumanTaskInstance> tareas = getTareasCasos(casosPrioritarios);


        if (tareas != null && tareas.size() > 0) {
            task = pesarOrdenarYAsignar(tareas, params);
            if (task != null) {
                LOGGER.error("Devolviendo caso prioritario");
                return task;
            }
        }

        final List<Long> casosReconsiderados = getCasosReconsiderados(params);
        tareas = getTareasCasos(casosReconsiderados);

        if (tareas != null && tareas.size() > 0) {
            task = pesarOrdenarYAsignar(tareas, params);
            if (task != null) {
                LOGGER.error("Devolviendo caso reconsiderado");
                return task;
            }
        }
        final List<Long> casosParametria = getCasosParametria(params);
        tareas = getTareasCasos(casosParametria);

        if (tareas != null && tareas.size() > 0) {
            task = pesarOrdenarYAsignar(tareas, params);
            if (task != null) {
                LOGGER.error("Devolviendo caso estandard");
                return task;
            }
        }
        LOGGER.error("La parametria utilizada no devuelve ninguna tarea, asignando la siguiente tarea por fecha");
        tareas = getTareasCasos(null);
        if (tareas != null && tareas.size() > 0) {
            task = asignarTarea(tareas);
            if (task != null) {
                LOGGER.error("Devolviendo cualquier tarea");
                return task;
            }
        }
        LOGGER.error("No hay tareas en el sistema");
        return null;
    }

    private Map<String, Serializable> pesarOrdenarYAsignar(List<HumanTaskInstance> tareas, Parametria params) {
        Map<String, Serializable> task = null;

        Map<Long, List<HumanTaskInstance>> mapaTareas = new HashMap<>();
        for(HumanTaskInstance tarea : tareas) {
            Long caseId = tarea.getRootContainerId();
            List<HumanTaskInstance> listaTareas = mapaTareas.get(caseId);
            if (listaTareas == null) {
                listaTareas = new ArrayList<>();
            }
            listaTareas.add(tarea);
            mapaTareas.put(caseId,listaTareas);
        }
        //Recuperamos los nombres de atributos que necesitamos
        List<Prioridad> prioridades = params.getPrioridadList();
        List<Orden> ordenes = params.getOrdenList();
        Set<String> clavesTemp = new HashSet<>();
        if(prioridades != null) {
            LOGGER.debug("Hay Prioridades");
            for (Prioridad p : prioridades) {
                clavesTemp.add(p.getPrioridad());
            }
        }
        if(ordenes != null) {
            LOGGER.debug("Hay Ordenes");
            for (Orden o : ordenes) {
                clavesTemp.add(o.getOrden());
            }
        }
        LOGGER.debug("Claves " + clavesTemp);
        if(clavesTemp.size() == 0){
            LOGGER.debug("No hay claves, se otorgara la tarea más antigua");
            task = asignarTarea(tareas);

        }else {
            List<String> claves = new ArrayList<>();
            claves.addAll(clavesTemp);
            //Construimos el sql
            String sqlSelect = "SELECT a.id_caso ID_CASO, ";
            String sqlFrom = " FROM ";
            String sqlWhere1 = " WHERE a.id_caso in (";
            StringBuilder sb = new StringBuilder(sqlWhere1);
            for (Long idCaso : mapaTareas.keySet()) {
                sb.append("'").append(idCaso).append("',");
            }
            sqlWhere1 = sb.toString();
            sqlWhere1 = sqlWhere1.substring(0, sqlWhere1.length() - 1);

            String sqlWhere2 = " ) and ";
            String sqlWhere3 = "";
            int caracter = 97;
            int numClaves = claves.size();
            for (int i = 0; i < numClaves; i++) {
                String clave = claves.get(i);
                String alias = Character.toString((char) (i + caracter));
                sqlSelect += alias + ".valor as " + clave + ",";
                sqlFrom += "PROCESSSEARCHINDEXES " + alias + ",";
                if (i > 0) {
                    sqlWhere2 += "a.id_caso = " + alias + ".id_caso and ";
                }
                sqlWhere3 += alias + ".clave = '" + clave + "' and ";
            }
            sqlSelect = sqlSelect.substring(0, sqlSelect.length() - 1);
            sqlFrom = sqlFrom.substring(0, sqlFrom.length() - 1);
            sqlWhere3 = sqlWhere3.substring(0, sqlWhere3.length() - 4);


            String sql = sqlSelect + sqlFrom + sqlWhere1 + sqlWhere2 + sqlWhere3;
            LOGGER.debug("ORDEN Y FILTRO: " + sql);

            Connection conBDM = null;
            Statement st = null;
            ResultSet rs = null;
            List<Map<String, String>> resultado = null;
            try {
                conBDM = dsBDM.getConnection();
                st = conBDM.createStatement();
                rs = st.executeQuery(sql);

                resultado = resultSetToList(rs);

            } catch (SQLException sqle) {
                LOGGER.error(sqle.getMessage());
            } finally {
                if (rs != null) {
                    try {
                        rs.close();
                    } catch (SQLException e) { /* ignored */}
                }
                if (st != null) {
                    try {
                        st.close();
                    } catch (SQLException e) { /* ignored */}
                }
                if (conBDM != null) {
                    try {
                        conBDM.close();
                    } catch (SQLException e) { /* ignored */}
                }

            }


            // Aplicar prioridad
            SortedMap<Integer, List<Map>> sortedMap = new TreeMap();
            if(resultado == null){
                LOGGER.error("El resultado es nulo ", claves, sql);
                return null;
            }
            for (Map<String, String> row : resultado) {

                Integer pesoActual = 0;
                for (Prioridad prioridad : prioridades) {
                    String valorCaso = row.get(prioridad.getPrioridad().toUpperCase());
                    String valorReferencia = prioridad.getValor()[0];
                    if (isInteger(valorCaso) && isInteger(valorReferencia)) {
                        switch (prioridad.getCondicion()) {
                            case "BETWEEN":
                                String valorReferencia2 = prioridad.getValor()[1];
                                if(isInteger(valorReferencia2)) {
                                    pesoActual += (Integer.parseInt(valorCaso) >= Integer.parseInt(valorReferencia2) && Integer.parseInt(valorCaso) <= Integer.parseInt(valorReferencia)) ? pesoActual + prioridad.getPeso() : pesoActual;
                                }
                                break;
                            //case "IN":
                            case ">":
                                pesoActual += Integer.parseInt(valorCaso) > Integer.parseInt(valorReferencia) ? pesoActual + prioridad.getPeso() : pesoActual;
                                break;
                            case ">=":
                                pesoActual += Integer.parseInt(valorCaso) >= Integer.parseInt(valorReferencia) ? pesoActual + prioridad.getPeso() : pesoActual;
                                break;
                            case "<":
                                pesoActual += Integer.parseInt(valorCaso) < Integer.parseInt(valorReferencia) ? pesoActual + prioridad.getPeso() : pesoActual;
                                break;
                            case "<=":
                                pesoActual += Integer.parseInt(valorCaso) <= Integer.parseInt(valorReferencia) ? pesoActual + prioridad.getPeso() : pesoActual;
                                break;
                            case "=":
                                pesoActual += Integer.parseInt(valorCaso) == Integer.parseInt(valorReferencia) ? pesoActual + prioridad.getPeso() : pesoActual;
                                break;

                        }
                    } else {
                        if (prioridad.getCondicion().equals("=")) {
                            pesoActual += valorCaso.equals(valorReferencia) ? pesoActual + prioridad.getPeso() : pesoActual;
                        } else {
                            if (prioridad.getCondicion().equals("IN")) {
                                boolean match = false;
                                for(String valor : prioridad.getValor()){
                                    if(valorCaso.equals(valor)) {
                                        match = true;
                                        break;
                                    }
                                }

                                pesoActual += match ? pesoActual + prioridad.getPeso() : pesoActual;
                            } else {
                                if (prioridad.getCondicion().equals("!=")) {
                                    boolean match = true;
                                    for(String valor : prioridad.getValor()){
                                        if(valorCaso.equals(valor)) {
                                            match = false;
                                            break;
                                        }
                                    }
                                    pesoActual += match ? pesoActual + prioridad.getPeso() : pesoActual;
                                } else {
                                    LOGGER.error("Criterio de prioridad no valido");
                                }
                            }
                        }
                    }
                }
                List<Map> tempList = sortedMap.get(pesoActual);
                if (tempList == null) {
                    tempList = new ArrayList<>();
                }
                tempList.add(row);
                sortedMap.put(pesoActual, tempList);
            }

            ArrayList<Integer> keys = new ArrayList<Integer>(sortedMap.keySet());
            for (int i = keys.size() - 1; i >= 0; i--) {
                List<Map> casos = sortedMap.get(keys.get(i));
                if (ordenes != null && ordenes.size() > 0) {
                    ArrayList<Map> casosOrdenados = new ArrayList<>();
                    for (Orden orden : ordenes) {
                        Comparator comparator = orden.getCriterio().equals("ASC") ? new IndexesASCComparator(orden.getOrden()) : new IndexesDESCComparator(orden.getOrden());
                        LOGGER.debug("COMPARADOR: " + comparator.getClass().getCanonicalName());
                        TreeSet<Map> casosAOrdenar = new TreeSet<Map>(comparator);
                        casosAOrdenar.addAll(casos);
                        LOGGER.debug("CASOS A ORDENAR: " + casos.toString());
                        casosOrdenados.addAll(casosAOrdenar);
                        LOGGER.debug("CASOS ORDENADOS: " + casosOrdenados.toString());
                    }
                    casos = casosOrdenados;
                }
                LOGGER.debug("CASOS: " + casos.toString());
                for (Map caso : casos) {

                    Long idCaso = Long.parseLong((String) caso.get("ID_CASO"));

                    List<HumanTaskInstance> listaTareasDelCaso = mapaTareas.get(idCaso);
                    if (listaTareasDelCaso != null) {
                        task = asignarTarea(listaTareasDelCaso);
                        if (task != null)
                            break;
                    } else {
                        LOGGER.error("No hay tareas disponibles para este caso y usuario");
                    }
                }
                if (task != null)
                    break;

            }
        }


        return task;
    }

    private boolean isInteger(String s) {
        try {
            Integer.parseInt(s);
        } catch(NumberFormatException e) {
            return false;
        } catch(NullPointerException e) {
            return false;
        }
        return true;
    }

    /**
     * Convert the ResultSet to a List of Maps, where each Map represents a row with columnNames and columnValues
     * @param rs
     * @return
     * @throws SQLException
     */
    private List<Map<String, String>> resultSetToList(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int columns = md.getColumnCount();
        List<Map<String, String>> rows = new ArrayList<Map<String, String>>();
        while (rs.next()){
            Map<String, String> row = new HashMap<String, String>(columns);
            for(int i = 1; i <= columns; ++i){
                row.put(md.getColumnLabel(i), rs.getString(i));
            }
            rows.add(row);
        }
        return rows;
    }

    private Parametria getParametria(String categoria) throws IOException {
        return ServicesAccessor.getUserParametria(idUsuario,categoria, REST_ENDPOINT);

    }



    private List<Long> getCasosPrioritarios(Parametria params) {
        List<Long> casos = new ArrayList<Long>();
        String sql = "SELECT p.id_caso FROM PROCESSSEARCHINDEXES p JOIN (SELECT id_caso, count(1) as num FROM " +
                "PROCESSSEARCHINDEXES WHERE id_caso in (SELECT prio.id_caso FROM PROCESSSEARCHINDEXES prio WHERE " +
                "(prio.cat_proceso = '"+ categoria +"' AND prio.CLAVE = 'statusPrioridad' AND to_char(prio.VALOR)='prioritario')) AND (";
        final List<Restriccion> listRestriccion = params.getRestriccionList();
        final int sizeRestriccion = listRestriccion.size();
        if(sizeRestriccion == 0){
            sql+="1 = 1";
        }else {
            for (int i = 0; i < sizeRestriccion; i++) {
                Restriccion r = listRestriccion.get(i);
                sql += r.getSQL();
                if (sizeRestriccion - 1 > i)
                    sql += " OR ";
            }
        }
        sql += ") group by id_caso having count(1) = " + sizeRestriccion + ") p2 ON (p.id_caso =  p2.id_caso ) " +
                "GROUP BY p.id_caso ";

        LOGGER.debug("PRIORITARIO SQL: " + sql);

        Connection conBDM = null;
        Statement st = null;
        ResultSet rs = null;
        try {
            conBDM = dsBDM.getConnection();
            st = conBDM.createStatement();
            rs = st.executeQuery(sql);

            while (rs.next()) {
                casos.add(rs.getLong("id_caso"));
            }
            return casos;
        } catch (SQLException sqle) {
            LOGGER.error(sqle.getMessage());
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) { /* ignored */}
            }
            if (st != null) {
                try {
                    st.close();
                } catch (SQLException e) { /* ignored */}
            }
            if (conBDM != null) {
                try {
                    conBDM.close();
                } catch (SQLException e) { /* ignored */}
            }

        }
        return casos;


    }

   private List<Long> getCasosReconsiderados(Parametria params) {
        List<Long> casos = new ArrayList<Long>();

        //ELIMINAR
        if(params!=null)
            return casos;


        String sql = "SELECT p.id_caso FROM PROCESSSEARCHINDEXES p JOIN (SELECT id_caso, count(1) as num FROM PROCESSSEARCHINDEXES WHERE ";
        sql += "cat_proceso = '"+ categoria +"' AND (";
        final List<Restriccion> listRestriccion = params.getRestriccionList();
        final int sizeRestriccion = listRestriccion.size();
       if(sizeRestriccion == 0){
           sql+="1 = 1";
       }else {
           for (int i = 0; i < sizeRestriccion; i++) {
               Restriccion r = listRestriccion.get(i);
               sql += r.getSQL();
               if (sizeRestriccion - 1 > i)
                   sql += " OR ";
           }
       }
        sql += " group by id_caso having num = " + sizeRestriccion + ") as p2 ON (p.id_caso =  p2.id_caso ) GROUP BY p.id_caso ";

        LOGGER.debug("RECONSIDERADO SQL: " + sql);

        Connection conBDM = null;
        Statement st = null;
        ResultSet rs = null;
        try {
            conBDM = dsBDM.getConnection();
            st = conBDM.createStatement();
            rs = st.executeQuery(sql);

            while (rs.next()) {
                casos.add(rs.getLong("id_caso"));
            }
            return casos;
        } catch (SQLException sqle) {
            LOGGER.error(sqle.getMessage());
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) { /* ignored */}
            }
            if (st != null) {
                try {
                    st.close();
                } catch (SQLException e) { /* ignored */}
            }
            if (conBDM != null) {
                try {
                    conBDM.close();
                } catch (SQLException e) { /* ignored */}
            }

        }
        return casos;

    }

    private List<Long> getCasosParametria(Parametria params) {
        List<Long> casos = new ArrayList<Long>();


        String sql = "SELECT p.id_caso FROM PROCESSSEARCHINDEXES p JOIN (SELECT id_caso, count(1) as num FROM " +
                "PROCESSSEARCHINDEXES WHERE (cat_proceso = '"+ categoria +"') and " +
                " id_caso not in (SELECT others.id_caso FROM PROCESSSEARCHINDEXES others WHERE" +
                " (others.CLAVE = 'statusPrioridad' AND to_char(others.VALOR)='prioritario') or " +
                "(others.CLAVE = 'reconsideracion' AND to_char(others.VALOR)!='')) and  ( ";
        final List<Restriccion> listRestriccion = params.getRestriccionList();
        final int sizeRestriccion = listRestriccion.size();
        if(sizeRestriccion == 0){
            sql+="1 = 1";
        }else {
            for (int i = 0; i < sizeRestriccion; i++) {
                Restriccion r = listRestriccion.get(i);
                sql += r.getSQL();
                if (sizeRestriccion - 1 > i)
                    sql += " OR ";
            }
        }

        sql += ") group by id_caso having count(1) = " + sizeRestriccion + ") p2 ON (p.id_caso =  p2.id_caso ) GROUP BY p.id_caso ";

        LOGGER.debug("ESTANDARD SQL: " + sql);

        Connection conBDM = null;
        Statement st = null;
        ResultSet rs = null;
        try {
            conBDM = dsBDM.getConnection();
            st = conBDM.createStatement();
            rs = st.executeQuery(sql);

            while (rs.next()) {
                casos.add(rs.getLong("id_caso"));
            }
            return casos;
        } catch (SQLException sqle) {
            LOGGER.error(sqle.getMessage());
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) { /* ignored */}
            }
            if (st != null) {
                try {
                    st.close();
                } catch (SQLException e) { /* ignored */}
            }
            if (conBDM != null) {
                try {
                    conBDM.close();
                } catch (SQLException e) { /* ignored */}
            }

        }
        return casos;

    }

    private List<HumanTaskInstance> getTareasCasosHeavy(List<Long> idsCaso) {


        List<HumanTaskInstance> tareas = null;
        final List<HumanTaskInstance> tareasOutput = new ArrayList<>();
        int count = 0;
        final int page = 500;
        boolean loop = true;
        while(loop) {
            try {
                ProcessAPI processAPI = getProcessAPI();
                SearchOptionsBuilder sob = new SearchOptionsBuilder(count, page);
                sob.sort(HumanTaskInstanceSearchDescriptor.REACHED_STATE_DATE, Order.ASC);
                tareas  = processAPI.searchPendingTasksForUser(idUsuario, sob.done()).getResult();
                for (HumanTaskInstance tarea : tareas) {
                    final Long caseId = tarea.getRootContainerId();
                    if (idsCaso.contains(caseId)) {
                        tareasOutput.add(tarea);
                    }
                }
                if( page > tareas.size() || tareasOutput.size() > 20){
                    loop = false;
                }else{
                    count += page;
                }
            } catch (SearchException e) {
                LOGGER.error("Excepción realizando la busqueda(heavy)", e);
            }
        }
        return tareasOutput;
    }

    private List<HumanTaskInstance> getTareasCasos(List<Long> idsCaso) {
        List<HumanTaskInstance> tareas = null;

        if(idsCaso != null && idsCaso.size() == 0) {
            LOGGER.debug("No hay casos sobre los que buscar");
            return tareas;
        }else{
            LOGGER.debug("getTareasCasos de "+idsCaso.size() + " casos");
            if(idsCaso != null && idsCaso.size() > 20){
                return getTareasCasosHeavy(idsCaso);
            }
        }
        try {
            ProcessAPI processAPI = getProcessAPI();
            SearchOptionsBuilder sob = new SearchOptionsBuilder(0, 200);
            if(idsCaso!= null) {
                boolean firstTime = true;
                for (Long caseId : idsCaso) {
                    if (!firstTime) {
                        sob.or();
                    } else {
                        firstTime = false;
                    }
                    sob.filter(HumanTaskInstanceSearchDescriptor.PROCESS_INSTANCE_ID, caseId);
                }
            }
            sob.sort(HumanTaskInstanceSearchDescriptor.REACHED_STATE_DATE, Order.ASC);
            tareas = processAPI.searchPendingTasksForUser(idUsuario, sob.done()).getResult();

        } catch (SearchException e) {
            LOGGER.error("Excepción realizando la busqueda", e);
        }
        return tareas;
    }

    private Map<String, Serializable> asignarTarea(List<HumanTaskInstance> tareas) {
        ProcessAPI p = getProcessAPI();
        Map<String, Serializable> tarea = null;
        for (HumanTaskInstance iter : tareas) {
            try {
                //Cambiar por el metodo especifico
                p.assignUserTaskIfNotAssigned(iter.getId(), idUsuario);
                tarea = new HashMap<String, Serializable>();
                tarea.put("id", iter.getId());
                tarea.put("caseId", iter.getParentProcessInstanceId());
                tarea.put("name", iter.getName());
                break;
            } catch (UpdateException e){
                LOGGER.info("No se puede asignar esta tarea",e.getMessage());
                continue;
            }
        }
        return tarea;
    }


    private ProcessAPI getProcessAPI() {
        ProcessAPI processAPI = null;
        try {

            processAPI = TenantAPIAccessor.getProcessAPI(session);
            processAPI.getNumberOfCategories();
        } catch (InvalidSessionException e) {
            LOGGER.debug("Sesion invalida, regenerando la sesion");
            try {
                session = TenantAPIAccessor.getLoginAPI().login(BONITA_TECH_USER, BONITA_TECH_USER_PASSWORD);
                processAPI = TenantAPIAccessor.getProcessAPI(session);
            } catch (Exception ex) {
                LOGGER.error("Exception", ex);
            }
        } catch (Exception e) {
            LOGGER.error("Exception", e);
        }
        return processAPI;


    }


    private static DataSource getDatasouce(String datasourceBDM) throws NamingException {
        Context ctx = new InitialContext();
        DataSource ds = (DataSource)ctx.lookup(datasourceBDM);
        return ds;
    }
}
