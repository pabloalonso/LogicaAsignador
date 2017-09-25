package com.bonitasoft.bbva.asignador;

import com.bonitasoft.bbva.asignador.beans.Orden;
import com.bonitasoft.bbva.asignador.beans.Parametria;
import com.bonitasoft.bbva.asignador.beans.Prioridad;
import com.bonitasoft.bbva.asignador.beans.Restriccion;
import com.bonitasoft.bbva.asignador.utils.IndexesASCComparator;
import com.bonitasoft.bbva.asignador.utils.IndexesDESCComparator;
import org.apache.commons.dbcp.BasicDataSource;
import org.bonitasoft.engine.api.ApiAccessType;
import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.api.TenantAPIAccessor;
import org.bonitasoft.engine.bpm.flownode.HumanTaskInstance;
import org.bonitasoft.engine.bpm.flownode.HumanTaskInstanceSearchDescriptor;
import org.bonitasoft.engine.exception.SearchException;
import org.bonitasoft.engine.search.Order;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.session.APISession;
import org.bonitasoft.engine.session.InvalidSessionException;
import org.bonitasoft.engine.util.APITypeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.*;
import java.util.*;


/**
 * @author Pablo Alonso de Linaje Garcia
 */
public class Asignador {
    private static final String BONITA_URL = "http://localhost:8080";
    private static final String BONITA_APP_NAME = "bonita";
    private static final String BONITA_TECH_USER = "install";
    private static final String BONITA_TECH_USER_PASSWORD = "install";
    private static final Logger LOGGER = LoggerFactory.getLogger(Asignador.class);
    private static Map<Long, Asignador> asignadorUsuario = new HashMap<Long, Asignador>();
    private DataSource dsBDM;
    private Long idUsuario;
    private APISession session;

    private Asignador() {
        super();
    }

    private Asignador(Long idUsuario, String datasourceBDM) {
        super();
        if(!datasourceBDM.equals("rest")) {
            Map<String, String> settings = new HashMap<String, String>();
            settings.put("server.url", BONITA_URL);
            settings.put("application.name", BONITA_APP_NAME);
            APITypeManager.setAPITypeAndParams(ApiAccessType.HTTP, settings);
        }
        this.idUsuario = idUsuario;
        this.dsBDM = getDatasouce(datasourceBDM);

    }

    public static Asignador getAsignador(Long idUser, String datasourceBDM) {
        Asignador asignador = asignadorUsuario.get(idUser);
        if (asignador == null) {
            asignador = new Asignador(idUser, datasourceBDM);
            asignadorUsuario.put(idUser, asignador);
        }
        return asignador;
    }


    public Map<String, Serializable> getNextTask() {

        Map<String, Serializable> task = new HashMap<String, Serializable>();
        Parametria params = getParametria();

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
        //Recuperamos los ids de los casos afectados
        List<Long> idsCasos = new ArrayList<>();

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
            for (Long idCaso : mapaTareas.keySet()) {
                sqlWhere1 += "'" + idCaso + "',";
            }
            sqlWhere1 = sqlWhere1.substring(0, sqlWhere1.length() - 1);

            String sqlWhere2 = " ) and ";
            String sqlWhere3 = "";
            int caracter = 97;
            int numClaves = claves.size();
            for (int i = 0; i < numClaves; i++) {
                String clave = claves.get(i);
                String alias = Character.toString((char) (i + caracter));
                sqlSelect += alias + ".valor as " + clave + ",";
                sqlFrom += "PROCESSSEARCHINDEXES as " + alias + ",";
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
            for (Map<String, String> row : resultado) {

                Integer pesoActual = 0;
                for (Prioridad prioridad : prioridades) {
                    String valorCaso = row.get(prioridad.getPrioridad().toUpperCase());
                    String valorReferencia = prioridad.getValor();
                    if (isInteger(valorCaso) && isInteger(valorReferencia)) {
                        switch (prioridad.getCondicion()) {
                            //case "BETWEEN":
                            //case "IN":
                            case ">":
                                pesoActual += Integer.parseInt(valorCaso) > Integer.parseInt(valorReferencia) ? pesoActual + prioridad.getPeso() : pesoActual;
                                break;
                            case ">=":
                                pesoActual += Integer.parseInt(valorCaso) >= Integer.parseInt(valorReferencia) ? pesoActual + prioridad.getPeso() : pesoActual;
                                break;
                            case "<":
                                pesoActual += Integer.parseInt(valorCaso) > Integer.parseInt(valorReferencia) ? pesoActual + prioridad.getPeso() : pesoActual;
                                break;
                            case "<=":
                                pesoActual += Integer.parseInt(valorCaso) >= Integer.parseInt(valorReferencia) ? pesoActual + prioridad.getPeso() : pesoActual;
                                break;
                            case "=":
                                pesoActual += Integer.parseInt(valorCaso) == Integer.parseInt(valorReferencia) ? pesoActual + prioridad.getPeso() : pesoActual;
                                break;

                        }
                    } else {
                        if (prioridad.getCondicion().equals("=")) {
                            pesoActual += valorCaso.equals(valorReferencia) ? pesoActual + prioridad.getPeso() : pesoActual;
                        } else {
                            LOGGER.error("Criterio de prioridad no valido");
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

            //List<HumanTaskInstance> tareasOrdenadas = new ArrayList<>();

            ArrayList<Integer> keys = new ArrayList<Integer>(sortedMap.keySet());
            for (int i = keys.size() - 1; i >= 0; i--) {
                List<Map> casos = sortedMap.get(keys.get(i));
                if (ordenes != null && ordenes.size() > 0) {
                    ArrayList<Map> casosOrdenados = new ArrayList<>();
                    for (Orden orden : ordenes) {
                        Comparator comparator = orden.getCriterio().equals("ASC") ? new IndexesASCComparator(orden.getOrden()) : new IndexesDESCComparator(orden.getOrden());
                        TreeSet<Map> casosAOrdenar = new TreeSet<Map>(comparator);
                        casosAOrdenar.addAll(casos);
                        casosOrdenados.addAll(casosAOrdenar);
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
                        //tareasOrdenadas.addAll(listaTareasDelCaso);
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
    private Parametria getParametria() {
        Parametria parametria = new Parametria();
        List<Restriccion> listaRestricciones = new ArrayList<Restriccion>();
        Restriccion rest = new Restriccion();
        rest.setRestriccion("Monto");
        rest.setCondicion("BETWEEN");
        rest.setValores("1025,1090");
        listaRestricciones.add(rest);
/*
        rest = new Restriccion();
        rest.setRestriccion("TipoCliente");
        rest.setCondicion("=");
        rest.setValor("Pensionado");
        listaRestricciones.add(rest);
*/
        List<Prioridad> prioridades = new ArrayList<>();
        Prioridad p = new Prioridad();
        p.setPrioridad("Producto");
        p.setValor("Vehiculos");
        p.setCondicion("=");
        p.setPeso(50);
        prioridades.add(p);

        p = new Prioridad();
        p.setPrioridad("TipoCliente");
        p.setValor("Pensionado");
        p.setCondicion("=");
        p.setPeso(60);
        prioridades.add(p);

        List<Orden> ordenes = new ArrayList<>();
        Orden o = new Orden();
        o.setCriterio("DESC");
        o.setOrden("Monto");
        ordenes.add(o);

        parametria.setOrdenList(ordenes);
        parametria.setPrioridadList(prioridades);
        parametria.setRestriccionList(listaRestricciones);

        return parametria;
    }

    private List<Long> getCasosPrioritarios(Parametria params) {
        List<Long> casos = new ArrayList<Long>();
        String sql = "SELECT p.id_caso FROM PROCESSSEARCHINDEXES p JOIN (SELECT id_caso, count(1) as num FROM " +
                "PROCESSSEARCHINDEXES WHERE id_caso in (SELECT prior.id_caso FROM PROCESSSEARCHINDEXES prior WHERE " +
                "(prior.CLAVE = 'statusPrioridad' AND prior.VALOR='prioritario')) AND (";
        final List<Restriccion> listRestriccion = params.getRestriccionList();
        final int sizeRestriccion = listRestriccion.size();
        for (int i = 0; i < sizeRestriccion; i++) {
            Restriccion r = listRestriccion.get(i);
            sql += r.getSQL();
            if (sizeRestriccion - 1 > i)
                sql += " OR ";
        }

        sql += ") group by id_caso having num = " + sizeRestriccion + ") as p2 ON (p.id_caso =  p2.id_caso ) " +
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
        final List<Restriccion> listRestriccion = params.getRestriccionList();
        final int sizeRestriccion = listRestriccion.size();

        for (int i = 0; i < sizeRestriccion; i++) {
            Restriccion r = listRestriccion.get(i);
            sql += r.getSQL();
            if (sizeRestriccion - 1 > i)
                sql += " OR ";
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
                "PROCESSSEARCHINDEXES WHERE id_caso not in (SELECT others.id_caso FROM PROCESSSEARCHINDEXES others WHERE" +
                " (others.CLAVE = 'statusPrioridad' AND others.VALOR='prioritario') or " +
                "(others.CLAVE = 'reconsideracion' AND others.VALOR!='')) and ( ";
        final List<Restriccion> listRestriccion = params.getRestriccionList();
        final int sizeRestriccion = listRestriccion.size();

        for (int i = 0; i < sizeRestriccion; i++) {
            Restriccion r = listRestriccion.get(i);
            sql += r.getSQL();
            if (sizeRestriccion - 1 > i)
                sql += " OR ";
        }

        sql += ") group by id_caso having num = " + sizeRestriccion + ") as p2 ON (p.id_caso =  p2.id_caso ) GROUP BY p.id_caso ";

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

    private List<HumanTaskInstance> getTareasCasos(List<Long> idsCaso) {
        List<HumanTaskInstance> tareas = null;
        if(idsCaso!= null && idsCaso.size() == 0) {
            return tareas;
        }
        try {
            ProcessAPI processAPI = getProcessAPI();
            SearchOptionsBuilder sob = new SearchOptionsBuilder(0, 50);
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
                p.assignUserTask(iter.getId(), idUsuario);
                tarea = new HashMap<String, Serializable>();
                tarea.put("id", iter.getId());
                tarea.put("caseId", iter.getParentProcessInstanceId());
                tarea.put("name", iter.getName());
                break;
            } catch (Exception e) {
                //Cambiar por la excepción especifica
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

    /**
     * METODO TEMPORAL
     *
     * @param datasourceBDM
     * @return
     */
    private DataSource getDatasouce(String datasourceBDM) {
        BasicDataSource basicDataSource = new BasicDataSource();
        basicDataSource.setDriverClassName("org.h2.Driver");
        basicDataSource.setUrl("jdbc:h2:file:C:\\BonitaBPM\\workspace\\BBVA-AsignadorTareas\\h2_database//business_data.db;MVCC=TRUE;DB_CLOSE_ON_EXIT=FALSE;IGNORECASE=TRUE;AUTO_SERVER=TRUE;");
        basicDataSource.setUsername("sa");
        basicDataSource.setPassword("");
        return basicDataSource;

    }
}