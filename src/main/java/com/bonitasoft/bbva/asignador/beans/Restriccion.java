package com.bonitasoft.bbva.asignador.beans;

/**
 * @author Pablo Alonso de Linaje García
 */
public class Restriccion {

    private String restriccion;
    private String condicion;
    private String[] valores;
    private String categoria;

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
        return this.valores[0];
    }

    public void setValor(String valor) {
        this.valores = new String[1];
        this.valores[0]= valor;
    }
    public void setValores(String valores){
        this.valores = valores.split(",");
    }
    public void setValores(String[] valores){
        this.valores = valores;
    }

    public String getSQL() {
        String sql ="";
        switch (condicion) {
            case "BETWEEN": sql += "( CLAVE='" + restriccion + "' and VALOR " + convertirCondicion(condicion) + " '" + valores[0] + "' and '" + valores[1] + "')";break;
            case "IN":      sql += "( CLAVE='" + restriccion + "' and VALOR " + convertirCondicion(condicion) + "(";
                    for(int i =0; i< valores.length; i++){
                        sql+="'" + valores[0] + "'";
                        if(valores.length-1<i){
                            sql+=",";
                        }
                    }
                    break;
            default: sql+="( CLAVE='" + restriccion + "' and VALOR " + convertirCondicion(condicion) + "'" + valores[0] + "')";
        }
        return sql;
    }
    private String convertirCondicion(String condicion) {
        condicion = condicion.trim().replaceAll(" ", "").toUpperCase();
        switch (condicion) {
            case "BETWEEN":
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

    public String getCategoria() {
        return categoria;
    }

    public void setCategoria(String categoria) {
        this.categoria = categoria;
    }
}
