package com.bonitasoft.bbva.asignador.beans;

/**
 * @author Pablo Alonso de Linaje García
 */
public class Prioridad {

    private String prioridad;
    private String condicion;
    private String[] valor;
    private Integer peso;


    public String getPrioridad() {
        return prioridad;
    }

    public void setPrioridad(String prioridad) {
        this.prioridad = prioridad;
    }

    public String getCondicion() {
        return condicion;
    }

    public void setCondicion(String condicion) {
        this.condicion = condicion;
    }

    public String[] getValor() {
        return valor;
    }

    public void setValor(String[] valor) {
        this.valor = valor;
    }

    public Integer getPeso() {
        return peso;
    }

    public void setPeso(Integer peso) {
        this.peso = peso;
    }
}
