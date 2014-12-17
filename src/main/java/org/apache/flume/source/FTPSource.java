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
import java.util.HashSet;
import java.util.Map;
import java.util.Iterator;

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
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.FileOutputStream;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

/*
 * @author Luis Lazaro
 */
public class FTPSource extends AbstractSource implements Configurable, PollableSource, Serializable {
    
    private static final Logger log = LoggerFactory.getLogger(FTPSource.class);
    private FTPSourceUtils ftpSourceUtils;
    private HashMap<File, Long> sizeFileList = new HashMap<>();
    private HashMap<File, Long> markFileList = new HashMap<>();
    private HashMap<File, Boolean> channelList = new HashMap<>();
    private HashSet<File> deletedFiles = new HashSet<>();
    private final int chunkSize = 1024;
       
    @Override
    public void configure(Context context) {            
        log.info("Reading and processing configuration values for source " + getName());
        ftpSourceUtils = new FTPSourceUtils(context);
        if (ftpSourceUtils.connectToserver()){
            log.info("Establishing connection to host " + ftpSourceUtils.getServer() + " for source  "  + getName());
        }
        log.info("Loading previous flumed data.....  " + getName());
        try {
                sizeFileList = loadMap("1hasmapS.ser");
                markFileList = loadMap("2hasmapS.ser");
                channelList = loadMapB("hasmapB.ser");
                
                for (File file : sizeFileList.keySet()){
                log.info("cargado sizeFileList :" + sizeFileList.get(file));
            }
                
                for (File file : markFileList.keySet()){
                log.info("cargado markFileList :" + markFileList.get(file));
            }
                
                for (File file : channelList.keySet()){
                log.info("cargado channelList :" + channelList.get(file));
            }
                
                closeChannel(channelList);
                
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
            return PollableSource.Status.READY;     //source was successfully able to generate events
        } catch(InterruptedException inte){
            inte.printStackTrace();
            return PollableSource.Status.BACKOFF;   //inform the runner thread to back off for a bit		
        }
    }

 
    public void start(Context context) {
        log.info("Starting sql source {} ...", getName());
        super.start();	    
    }
    

    @Override
    public void stop() {
            saveMap(sizeFileList, 1);
            saveMap(markFileList, 2);
            saveMapB(channelList);
            for (File file : sizeFileList.keySet()){
                log.info("save sizeFileList :" + sizeFileList.get(file));
            }
                
                for (File file : markFileList.keySet()){
                log.info("save markFileList :" + markFileList.get(file));
            }
                
                for (File file : channelList.keySet()){
                log.info("save channelList :" + channelList.get(file));
            }
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
    public void processMessage(byte[] lastInfo){
        byte[] message = lastInfo;
        Event event = new SimpleEvent();
        Map<String, String> headers =  new HashMap<String, String>();  
        headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
        event.setBody(message);
        event.setHeaders(headers);
        getChannelProcessor().processEvent(event);
    }
    
    
    /*
    @void retrieve files from directories
    */
    public void discoverElements(){
        try {  
            Path start = Paths.get(ftpSourceUtils.getFtpClient().printWorkingDirectory());  
  
            Files.walkFileTree(start, new SimpleFileVisitor<Path>() {  
                @Override  
                public FileVisitResult visitFile(final Path file, BasicFileAttributes attributes) throws IOException {
                        
                         cleanList(sizeFileList);
                         cleanListMirror(markFileList);
                         if (sizeFileList.containsKey(file.toFile())){                              // known file
                               final RandomAccessFile ranAcFile = new RandomAccessFile(file.toFile(), "r");                             
                               ranAcFile.seek(sizeFileList.get(file.toFile()));
                               long size = ranAcFile.length() - sizeFileList.get(file.toFile());
                               if (size > 0) {
                                   sizeFileList.put(file.toFile(), ranAcFile.length());
                                   log.info("Modified: " + file.getFileName() + "," + attributes.fileKey() + " ," + sizeFileList.size() +  " ,size: " + size +  " , Actual "  + ranAcFile.length() + ", Guardado " + sizeFileList.get(file.toFile()) + ", marca " + markFileList.get(file.toFile()));
                                   ReadFileWithFixedSizeBuffer(ranAcFile, file.toFile());
                                   
                                    
                               } else if (size == 0 ){ //known & NOT modified
                                   if (markFileList.get(file.toFile()) < ranAcFile.length() ){
                                       if (channelList.get(file.toFile())){
                                           log.info("channel open: " + file.getFileName()+ ", " + markFileList.get(file.toFile()) + " of  " + ranAcFile.length() + ", Guardado " + sizeFileList.get(file.toFile()) + ", marca " + markFileList.get(file.toFile()) );
                                           ranAcFile.close(); 
                                       } else {
                                           log.info("channel closed: " + file.getFileName()+ ", " + markFileList.get(file.toFile()) + " of  " + ranAcFile.length() + ", Guardado " + sizeFileList.get(file.toFile()) + ", marca " + markFileList.get(file.toFile()) );
                                          
                                           Thread threadReFile = new Thread( new Runnable(){
                                            @Override
                                            public void run(){
                                                try {
                                                    channelList.put(file.toFile(),true);
                                                    final RandomAccessFile ranAcFile = new RandomAccessFile(file.toFile(), "r");
                                                    ranAcFile.seek(markFileList.get(file.toFile()));
                                                    ReadFileWithFixedSizeBuffer(ranAcFile, file.toFile());
                                                    } catch(IOException e) {
                                                        e.printStackTrace();
                                                    }
                                            }
                                                    });
                                                    threadReFile.setName("hiloReFile_" + file.getFileName());
                                                    threadReFile.start();

                                           }
                                   }
                                   ranAcFile.close();
                               } else if (size < 0) { //known &  modified from offset 0
                                   ranAcFile.seek(0);
                                   sizeFileList.put(file.toFile(), ranAcFile.length());
                                   log.info("full modified: " + file.getFileName() + "," + attributes.fileKey() + " ," + sizeFileList.size());
                                   ReadFileWithFixedSizeBuffer(ranAcFile, file.toFile());
                               }
                               
                        } else {    //new File
                                    final RandomAccessFile ranAcFile = new RandomAccessFile(file.toFile(), "r");
                                    sizeFileList.put(file.toFile(), ranAcFile.length());
                                    channelList.put(file.toFile(),true);
                                    log.info("discovered: " + file.getFileName() + "," + attributes.fileKey() + " ," + sizeFileList.size() + " , Actual "  + ranAcFile.length() + ", guardo Ac " + sizeFileList.get(file.toFile()));
                                    Thread threadNewFile = new Thread( new Runnable(){
                                    @Override
                                    public void run(){
                                        try {
                                        ReadFileWithFixedSizeBuffer(ranAcFile, file.toFile());
                                        } catch(IOException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                });
                                    threadNewFile.setName("hiloNewFile_" + file.getFileName());
                                    threadNewFile.start();
                                    
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
    @void, delete file from hashmaps if deleted from ftp
    */
    public void cleanList(HashMap<File,Long> map) {
          for (Iterator<File> iter=map.keySet().iterator();iter.hasNext();) {
          final File file = iter.next();
          if (!(file.exists())){ 
              iter.remove();
              deletedFiles.add(file);
          }
        }
    }
    
    /*
    @void. Avoid double concurrent access
    */
    public void cleanListMirror(HashMap<File,Long> map){
        for (File file: deletedFiles){
            map.remove(file);
        }
    }
    
    /*
    @void Serialize hashmap
    */
    public void saveMap(HashMap<File, Long> map, int num){
        try { 
            FileOutputStream fileOut = new FileOutputStream(num + "hasmapS.ser");
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
    @void Serialize hashmap
    */
    public void saveMapB(HashMap<File, Boolean> map){
        try { 
            FileOutputStream fileOut = new FileOutputStream("hasmapB.ser");
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
        in.close();
        return hasMap;
        
    } 
    
        
    /*
    @return HashMap<File,Long> objects
    */
    public HashMap<File,Boolean> loadMapB(String name) throws ClassNotFoundException, IOException{
        FileInputStream map = new FileInputStream(name);
        ObjectInputStream in = new ObjectInputStream(map);
        HashMap hasMap = (HashMap)in.readObject();
        in.close();
        return hasMap;
        
    } 
    
    /*
    @void, Read a large file in chunks with fixed size buffer and process chunk
    */
    public void ReadFileWithFixedSizeBuffer(RandomAccessFile aFile) throws IOException{
        FileChannel inChannel = aFile.getChannel();
        ByteBuffer buffer = ByteBuffer.allocateDirect(chunkSize);
            while(inChannel.read(buffer) > 0)
            {
                FileLock lock = inChannel.lock(inChannel.position(), chunkSize, true);
                byte[] data = new byte[chunkSize];
                buffer.flip(); //alias for buffer.limit(buffer.position()).position(0)
                for (int i = 0;  i < buffer.limit();  i++)
                {
                    data[i] =buffer.get();
                }
                processMessage(data);
                buffer.clear(); // sets the limit to the capacity and the position back to 0 
                lock.release();
            }
        inChannel.close();
        aFile.close();
    }
    
    
    /*
    @void, Read a large file in chunks with fixed size buffer and process chunk
    */
    public void ReadFileWithFixedSizeBuffer(RandomAccessFile aFile, File file) throws IOException{
        FileChannel inChannel = aFile.getChannel();
        ByteBuffer buffer = ByteBuffer.allocateDirect(chunkSize);
            while(inChannel.read(buffer) > 0)
            {
                markFileList.put(file,inChannel.position());                
                FileLock lock = inChannel.lock(inChannel.position(), chunkSize, true);
                byte[] data = new byte[chunkSize];
                buffer.flip(); //alias for buffer.limit(buffer.position()).position(0)
                for (int i = 0;  i < buffer.limit();  i++)
                {
                    data[i] =buffer.get();
                }                
                processMessage(data);
                buffer.clear(); // sets the limit to the capacity and the position back to 0 
                lock.release();
            }
            
        channelList.put(file,false);    
        inChannel.close();
        aFile.close();
    }
    
    
    /*
    @return void, if any channel persists opened must be closed of reboot process
    */
    public void closeChannel(HashMap<File, Boolean> map){
        for (Iterator<File> iter=map.keySet().iterator();iter.hasNext();) {
          final File file = iter.next();
          if (map.get(file)){ 
              map.put(file, Boolean.FALSE);
          }
        }
    }
    
}
