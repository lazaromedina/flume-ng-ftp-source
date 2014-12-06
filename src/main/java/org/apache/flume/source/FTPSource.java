/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  
 * http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *******************************************************************************/
package org.apache.flume.source;


import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.FileNotFoundException;
import java.io.IOException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.io.File;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.FileVisitResult;

import java.io.RandomAccessFile;


import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader; 
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.FileOutputStream;

import java.io.Serializable;

/*
 * @author Luis Lazaro
 */
public class FTPSource extends AbstractSource implements Configurable, PollableSource, Serializable {
    
    private static final Logger log = LoggerFactory.getLogger(FTPSource.class);
    private FTPSourceUtils ftpSourceUtils;
    private HashMap<File, Long> sizeFileList = new HashMap<>();
    
    
   
       
    @Override
    public void configure(Context context) {            
        log.info("Reading and processing configuration values for source " + getName());
        ftpSourceUtils = new FTPSourceUtils(context);
        if (ftpSourceUtils.connectToserver()){
            log.info("Establishing connection to host " + ftpSourceUtils.getServer() + " for source  "  + getName());
        }
        log.info("Loading previous flumed data.....  " + getName());
        try {
                sizeFileList = loadMap("hasmap.ser");
                } catch (ClassNotFoundException | IOException e){
                    e.printStackTrace();
                    log.error("Fail to load previous flumed data.");
                }
    }
    
    /*
    @enum Status , process source configured from context
    */
    public PollableSource.Status process() throws EventDeliveryException {
        
       discoverElements();

        try 
        {  
            Thread.sleep(this.ftpSourceUtils.getRunDiscoverDelay());				
            return PollableSource.Status.READY;
        } catch(InterruptedException inte){
            inte.printStackTrace();
            return PollableSource.Status.BACKOFF;			
        }
    }

 
    public void start(Context context) {
        log.info("Starting sql source {} ...", getName());
        super.start();	    
    }
    

    @Override
    public void stop() {
            saveMap(sizeFileList);
            log.info("Stopping sql source {} ...", getName());
            try { 
                 ftpSourceUtils.getFtpClient().logout();
                 ftpSourceUtils.getFtpClient().disconnect();
            } catch (IOException ioe) {
                 super.stop();
                 ioe.printStackTrace();
            }
            super.stop();
    }
    
    
   
    /*
    @void process last append to files
    */
    public void processMessage(String lastInfo){
        byte[] message = lastInfo.getBytes(); 
        Event event = new SimpleEvent();
        Map<String, String> headers =  new HashMap<String, String>();  
        headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
        event.setBody(message);
        event.setHeaders(headers);
        getChannelProcessor().processEvent(event);
        log.info(lastInfo);
    }
    
    /*
    @void retrieve files from directories
    */
    public void discoverElements(){
        try {  
            Path start = Paths.get(ftpSourceUtils.getFtpClient().printWorkingDirectory());  
  
            Files.walkFileTree(start, new SimpleFileVisitor<Path>() {  
                @Override  
                public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                        
                         refreshList(sizeFileList);
                         if (sizeFileList.containsKey(file.toFile())){ // el archivo es conocido
                               RandomAccessFile ranAcFile = new RandomAccessFile(file.toFile(), "r");                             
                               ranAcFile.seek(sizeFileList.get(file.toFile()) - 1);
                               long size = ranAcFile.length() - sizeFileList.get(file.toFile());
                               if (size > 0) { //conocido y se ha modificado 
                                   byte[] data = new byte[(int) size];
                                   ranAcFile.read(data);
                                   String lastInfo = new String(data);
                                   processMessage("modified: " + file.getFileName() + "," +
                                                                 attributes.fileKey() + " ," +
                                                                 sizeFileList.size() + ": " +
                                                                 lastInfo
                                                                );
                                     sizeFileList.put(file.toFile(), ranAcFile.length());
                                     ranAcFile.close();
                                    
                               } else if (size == 0) { 
                                   ranAcFile.close();
                               } //no se ha modificado
                               
                        } else { //nuevo archivo encontrado
                                
                                RandomAccessFile ranAcFile = new RandomAccessFile(file.toFile(), "r");
                                if (ranAcFile.length() > 1000000){
                                    log.warn(file.getFileName() + " WARNING : excess initial size, calling butcher.. ");
                                   // ReadFileWithFixedSizeBuffer(ranAcFile);
                                   ranAcFile.close();
                                   return FileVisitResult.CONTINUE;
                                    
                                } else { //normal file
                                    sizeFileList.put(file.toFile(), ranAcFile.length());
                                    byte[] data = new byte[(int) ranAcFile.length()];
                                    ranAcFile.read(data);
                                    String lastInfo = new String(data);
                                    processMessage("discovered: " + file.getFileName() + "," +
                                                                     attributes.fileKey() + " ," +
                                                                     sizeFileList.size() + ": " +
                                                                      lastInfo
                                                                     );
                                    ranAcFile.close();
                                }
                        }
                    return FileVisitResult.CONTINUE;  
                
                }
                @Override  
                public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {  
                    return FileVisitResult.CONTINUE;  
                }  
            });  
        } catch (IOException ex) {  
          ex.printStackTrace();
        }  
    }
    
    
    
    /*
    @return string only last line from a file
    */
    public String getLastLine(File file) throws FileNotFoundException, IOException{
        FileInputStream in = new FileInputStream(file);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String strLine = null, tmp;
        while ((tmp = br.readLine()) != null) {
            strLine = tmp; 
        }
        in.close();
        String lastLine = strLine;
        return lastLine;
    }
    
    /*
    @void, delete file from hashmaps if deleted from ftp
    */
    public void refreshList(HashMap<File, Long> map) {
        for (File file: map.keySet()){
            if (!(file.exists())){
                map.remove(file);
            }
        }
    }
    
    /*
    @void Serialize hashmap
    */
    public void saveMap(HashMap<File, Long> map){
        try { 
            FileOutputStream fileOut = new FileOutputStream("hasmap.ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(map);
            out.close();
        } catch(FileNotFoundException e){
            e.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();
        }
    }
    
    
    /*
    @return HashMap<File,Long> objects
    */
    public HashMap<File,Long> loadMap(String name) throws ClassNotFoundException, IOException{
        FileInputStream map = new FileInputStream(name);
        ObjectInputStream in = new ObjectInputStream(map);
        HashMap hasMap = (HashMap)in.readObject();
        return hasMap;
        
    } 
    
}