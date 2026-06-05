package de.rwthaachen.dbis.i5cloudmatch.test;

import java.io.*;  
public class TestExec {  
    public static void main(String[] args) {  
        try {  
            Process p = Runtime.getRuntime().exec("wget http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.111.3313&xml=true");  
            BufferedReader in = new BufferedReader(  
                                new InputStreamReader(p.getInputStream()));  
            String line = null;  
            while ((line = in.readLine()) != null) {  
                System.out.println(line);  
            }  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
    }  
}