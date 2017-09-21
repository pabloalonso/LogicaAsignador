package com.bonitasoft.bbva.asignador.beans;

/**
 * Created by pablo on 20/09/2017.
 */
public abstract class ParametriaAsignador {

    public abstract String getSQL();

    protected String convertirCondicion(String condicion) {
        condicion = condicion.trim().replaceAll(" ", "").toUpperCase();
        switch (condicion) {
            case "IN":
            case ">":
            case ">=":
            case "=":
            case "<":
            case "<=":
                return condicion;
            case "=>":
                return ">=";
            case "=<":
                return "<=";
            case "==":
                return "=";
            default:
                return "=";
        }
    }
}
