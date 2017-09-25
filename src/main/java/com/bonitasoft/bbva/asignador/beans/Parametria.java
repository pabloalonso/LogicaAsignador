package com.bonitasoft.bbva.asignador.beans;

import java.util.List;

/**
 * @author Pablo Alonso de Linaje García
 */
public class Parametria {
    private List<Orden> ordenList;
    private List<Prioridad> prioridadList;
    private List<Restriccion> restriccionList;

    public List<Orden> getOrdenList() {
        return ordenList;
    }

    public void setOrdenList(List<Orden> ordenList) {
        this.ordenList = ordenList;
    }

    public List<Prioridad> getPrioridadList() {
        return prioridadList;
    }

    public void setPrioridadList(List<Prioridad> prioridadList) {
        this.prioridadList = prioridadList;
    }

    public List<Restriccion> getRestriccionList() {
        return restriccionList;
    }

    public void setRestriccionList(List<Restriccion> restriccionList) {
        this.restriccionList = restriccionList;
    }


}
