/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.flume.source;

import java.nio.file.Path;
import java.io.RandomAccessFile;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Objects;
/**
 *
 * @author luis
 */
public class FileSource {
    private Path path;
    private long startSize, prevSize, offset;
    private RandomAccessFile randAccess;
    private byte[] data ;
    private final String OPENMODE = "r";
    private String lastInfo;
    
    public FileSource(Path newPath){
        path = newPath;
        try {
            randAccess = new RandomAccessFile(path.toFile(), OPENMODE);            
        } catch(FileNotFoundException e){
            e.printStackTrace();
        }
        try {
            startSize = randAccess.length();
            byte[] data = new byte[(int) startSize];
            randAccess.read(data);
            lastInfo = new String(data);
        } catch(IOException e){
            e.printStackTrace();
        }
    }
    
    /*
    @return Path of this source file
    */
    public Path getFileSourcePath(){
        return path;
    }
    
    /*
    @void, set path for source
    */
    public void setFileSourcePath(Path newPath){
        path = newPath;
    }
    
    /*
    @Long, starting size of source file
    */
    public long getStartSize(){
        return startSize;
    }
    
    /*
    @void, set start size for source file
    */
    public void setStartSize(long newSize){
        startSize = newSize;
    }
    
    /*
    @Long, previous size of source file
    */
    public long getPrevSize(){
        return prevSize;
    }
    
    /*
    @void, set previous size for source file
    */
    public void setPrevSize(long newSize){
        prevSize = newSize;
    }
    
    /*
    @RandomAccesFile, direct acess for source file
    */
    public RandomAccessFile getRandAcessFileSource(){
        return randAccess;
    }
    
    public void loadDataFileSource(){
        try {
            randAccess.read(data);
        } catch (IOException e){
            e.printStackTrace();
        }
    }
    
    public String getLasInfoFileSource(){
        return lastInfo;
    }
    
    public void setLastInfoFileSource(byte[] data){
        lastInfo = new String(data);
    }
    
    
            
    @Override
    public int hashCode() {
        return path.hashCode();
    }

   

    


    
} //end of class
