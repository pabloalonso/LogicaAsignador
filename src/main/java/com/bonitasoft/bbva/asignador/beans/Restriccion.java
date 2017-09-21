package com.bonitasoft.bbva.asignador.beans;

/**
 * Created by pablo on 20/09/2017.
 */
public class Restriccion extends ParametriaAsignador {

    private String restriccion;
    private String condicion;
    private String valor;

    public String getRestriccion() {
        return restriccion;
    }

    public void setRestriccion(String restriccion) {
        this.restriccion = restriccion;
    }

    public String getCondicion() {
        return condicion;
    }

    public void setCondicion(String condicion) {
        this.condicion = condicion;
    }

    public String getValor() {
        return valor;
    }

    public void setValor(String valor) {
        this.valor = valor;
    }

    public String getSQL() {
        return "( CLAVE='" + restriccion + "' and VALOR " + convertirCondicion(condicion) + "'" + valor + "')";
    }
}
