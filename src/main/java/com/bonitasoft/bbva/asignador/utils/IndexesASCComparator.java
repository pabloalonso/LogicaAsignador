package com.bonitasoft.bbva.asignador.utils;

import java.util.Comparator;
import java.util.Map;

/**
 * @author Pablo Alonso de Linaje García
 */
public class IndexesASCComparator implements Comparator<Map> {

    private final String index;


    public IndexesASCComparator(String index){
        super();
        this.index = index.toUpperCase();
    }
    @Override
    public int compare(Map e1, Map e2) {
        return ((String)e1.get(index)).compareTo((String)e2.get(index));
    }


}
