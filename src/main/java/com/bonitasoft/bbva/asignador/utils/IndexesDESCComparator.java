package com.bonitasoft.bbva.asignador.utils;

import java.util.Comparator;
import java.util.Map;

/**
 * @author Pablo Alonso de Linaje García
 */
public class IndexesDESCComparator implements Comparator<Map> {

    private final String index;


    public IndexesDESCComparator(String index){
        super();
        this.index = index.toUpperCase();
    }
    @Override
    public int compare(Map e1, Map e2) {
        String v1 = (String)e1.get(index);
        String v2 = (String)e2.get(index);

        if (isInteger(v1) && isInteger(v2)) {
            return (new Integer(v2)).compareTo((new Integer(v1)));
        }else{

        }
        return v2.compareTo(v1);
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
}
