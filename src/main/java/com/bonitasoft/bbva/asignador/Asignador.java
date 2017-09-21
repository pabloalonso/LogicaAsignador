package com.bonitasoft.bbva.asignador;

import com.bonitasoft.bbva.asignador.beans.Parametria;
import com.bonitasoft.bbva.asignador.beans.Restriccion;
import org.apache.commons.dbcp.BasicDataSource;
import org.bonitasoft.engine.api.ApiAccessType;
import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.api.TenantAPIAccessor;
import org.bonitasoft.engine.bpm.flownode.HumanTaskInstance;
import org.bonitasoft.engine.bpm.flownode.HumanTaskInstanceSearchDescriptor;
import org.bonitasoft.engine.exception.SearchException;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.session.APISession;
import org.bonitasoft.engine.session.InvalidSessionException;
import org.bonitasoft.engine.util.APITypeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
        Map<String, String> settings = new HashMap<String, String>();
        settings.put("server.url", BONITA_URL);
        settings.put("application.name", BONITA_APP_NAME);
        APITypeManager.setAPITypeAndParams(ApiAccessType.HTTP, settings);
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
            task = asignarTarea(tareas);
            if (task != null) {
                LOGGER.error("Devolviendo caso prioritario");
                return task;
            }
        }
        final List<Long> casosReconsiderados = getCasosReconsiderados(params);
        tareas = getTareasCasos(casosReconsiderados);
        if (tareas != null && tareas.size() > 0) {
            task = asignarTarea(tareas);
            if (task != null) {
                LOGGER.error("Devolviendo caso reconsiderado");
                return task;
            }
        }
        final List<Long> casosParametria = getCasosParametria(params);
        tareas = getTareasCasos(casosParametria);
        if (tareas != null && tareas.size() > 0) {
            task = asignarTarea(tareas);
            if (task != null) {
                LOGGER.error("Devolviendo caso estandard");
                return task;
            }
        }
        return null;
    }


    private Parametria getParametria() {
        Parametria p = new Parametria();
        List<Restriccion> listaRestricciones = new ArrayList<Restriccion>();
        Restriccion rest = new Restriccion();
        rest.setRestriccion("Monto");
        rest.setCondicion(">");
        rest.setValor("1025");
        listaRestricciones.add(rest);

        rest = new Restriccion();
        rest.setRestriccion("Monto");
        rest.setCondicion("<");
        rest.setValor("1040");
        listaRestricciones.add(rest);
        p.setRestriccionList(listaRestricciones);
        return p;
    }

    private List<Long> getCasosPrioritarios(Parametria params) {
        List<Long> casos = new ArrayList<Long>();
        String sql = "SELECT p.id_caso FROM PROCESSSEARCHINDEXES p JOIN (SELECT id_caso, count(1) as num FROM PROCESSSEARCHINDEXES WHERE (CLAVE = 'statusPrioridad' AND VALOR='prioritario') OR ";
        final List<Restriccion> listRestriccion = params.getRestriccionList();
        final int sizeRestriccion = listRestriccion.size();
        Set<String> resSet = new HashSet<String>();
        for (int i = 0; i < sizeRestriccion; i++) {
            Restriccion r = listRestriccion.get(i);
            resSet.add(r.getRestriccion());
            sql += r.getSQL();
            if (sizeRestriccion - 1 > i)
                sql += " OR ";
        }

        sql += " group by id_caso having num = " + (resSet.size() + 1) + ") as p2 ON (p.id_caso =  p2.id_caso ) GROUP BY p.id_caso ";

        LOGGER.error("SQL: " + sql);

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


        String sql = "SELECT p.id_caso FROM PROCESSSEARCHINDEXES p JOIN (SELECT id_caso, count(1) as num FROM PROCESSSEARCHINDEXES WHERE ";
        final List<Restriccion> listRestriccion = params.getRestriccionList();
        final int sizeRestriccion = listRestriccion.size();
        Set<String> resSet = new HashSet<String>();
        for (int i = 0; i < sizeRestriccion; i++) {
            Restriccion r = listRestriccion.get(i);
            resSet.add(r.getRestriccion());
            sql += r.getSQL();
            if (sizeRestriccion - 1 > i)
                sql += " OR ";
        }

        sql += " group by id_caso having num = " + resSet.size() + ") as p2 ON (p.id_caso =  p2.id_caso ) GROUP BY p.id_caso ";

        LOGGER.debug("SQL: " + sql);

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


        String sql = "SELECT p.id_caso FROM PROCESSSEARCHINDEXES p JOIN (SELECT id_caso, count(1) as num FROM PROCESSSEARCHINDEXES WHERE ";
        final List<Restriccion> listRestriccion = params.getRestriccionList();
        final int sizeRestriccion = listRestriccion.size();
        Set<String> resSet = new HashSet<String>();
        for (int i = 0; i < sizeRestriccion; i++) {
            Restriccion r = listRestriccion.get(i);
            resSet.add(r.getRestriccion());
            sql += r.getSQL();
            if (sizeRestriccion - 1 > i)
                sql += " OR ";
        }

        sql += " group by id_caso having num = " + resSet.size() + ") as p2 ON (p.id_caso =  p2.id_caso ) GROUP BY p.id_caso ";

        LOGGER.debug("SQL: " + sql);

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
        try {
            ProcessAPI processAPI = getProcessAPI();
            SearchOptionsBuilder sob = new SearchOptionsBuilder(0, 10);
            boolean firstTime = true;
            for (Long caseId : idsCaso) {
                if (!firstTime) {
                    sob.or();
                } else {
                    firstTime = false;
                }
                sob.filter(HumanTaskInstanceSearchDescriptor.PROCESS_INSTANCE_ID, caseId);
            }

            tareas = processAPI.searchMyAvailableHumanTasks(idUsuario, sob.done()).getResult();

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
