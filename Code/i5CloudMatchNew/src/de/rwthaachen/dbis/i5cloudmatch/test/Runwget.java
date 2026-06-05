package de.rwthaachen.dbis.i5cloudmatch.test;

import java.io.*;
import java.io.IOException;

public class Runwget
{

  public static void main (String args[])
  {
    String whatToRun = "wget http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.111.3313&xml=true";
   try
   {
     Runtime rt = Runtime.getRuntime();
     Process proc = rt.exec(whatToRun);
     int exitVal = proc.waitFor();
     System.out.println("Process exitValue:" + exitVal);
   } catch (Throwable t)
     {
       t.printStackTrace();
     }
  }
}