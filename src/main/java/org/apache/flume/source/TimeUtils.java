/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.flume.source;

import java.text.SimpleDateFormat;
import java.util.Calendar;
/**
 *
 * @author Luis Alfonso
 */
public class TimeUtils {
    private Calendar fileCalendar;
    
    public TimeUtils(Calendar calendar){
        fileCalendar = calendar;
    }
    
    /*
     @return long date and time for now
     */
     public long getNowMinutes(){
         Calendar now = Calendar.getInstance();
         long nowInMiliseg = now.getTimeInMillis();
         long nowInMinutes = nowInMiliseg / (60 * 1000);
         return nowInMinutes;
     }
     
     /*
     @return long date and time for file's calendar
     */
     public long getFileMinutes(){
         return fileCalendar.getTimeInMillis()/(60 * 1000) + 60;
     }
     
     /*
     @return String fecha actual formateada
     */
     public String getNowDate(){
         Calendar now = Calendar.getInstance();
         SimpleDateFormat formatoFecha = new SimpleDateFormat(" HH:mm:ss   EEEEE  dd/MMM/yyy");
         return formatoFecha.format(now.getTime());
     }
     
     
     
     public String getFileDate(){
         return new SimpleDateFormat(" HH:mm:ss   EEEEE  dd/MMM/yyy").format(fileCalendar.getTime());
     }
     
     /*
     @return String hora actual formateada
     */
     public String getHoraActual(){
         Calendar ahora = Calendar.getInstance();
         SimpleDateFormat formatoHora = new SimpleDateFormat("HH:mm:ss");
         return formatoHora.format(ahora.getTime());
     }
     
     public String getFileHour(){
         return new SimpleDateFormat("HH:mm:ss").format(fileCalendar.getTime());
     }
     
     /*
     @return long horas
     */
     public long getHoras(long minutos){
         return minutos/60;
     }
     
     /*
     @return long dias
     */
     public long getDias(long minutos){
         return minutos/3600;
     }
     
     /*
     @return String la fecha en formato 
     */
     public String getDateTime(){
         Calendar ahora = Calendar.getInstance();
         SimpleDateFormat formatoFecha = new SimpleDateFormat("yyyyMMddHHmm");
         return formatoFecha.format(ahora.getTime()) + "%";
     }
     
     public long getElapsedMinutesFile(){
         return (getNowMinutes() - getFileMinutes());
     }
     
     
     public long getElapsedMsecondsFile(){
         return (Calendar.getInstance().getTimeInMillis() - fileCalendar.getTimeInMillis() - 3600000);
     }
     
     public Calendar getFileCalendar(){
         return fileCalendar;
     }
     
     public long getMsecondsFileCalendar(){
         return (fileCalendar.getTimeInMillis() - 3600000);
     }
    
}//fin de Tiempo
