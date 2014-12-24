/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.flume.source;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTP;

import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.net.ftp.FTPConnectionClosedException;

import java.io.IOException;



/**
 *
 * @author luis lazaro 
 */


public class FTPSourceUtils {
    private FTPClient ftpClient;
    private String server,user,password,defaultDirectory;
    private int runDiscoverDelay;
    private static final Logger log = LoggerFactory.getLogger(FTPSourceUtils.class);
    private boolean login;
    
    public FTPSourceUtils(Context context){
        ftpClient = new FTPClient();
        server = context.getString("name.server");
        user = context.getString("user");
        password = context.getString("password");
        runDiscoverDelay = context.getInteger("run.discover.delay");
        defaultDirectory = context.getString("server.directory");
    }
    
    /*
    @return boolean, Opens a Socket connected to a server
    and login to return True if successfully completed, false if not.
    */
    public boolean connectToserver(){
        try {
                ftpClient.connect(server);
                login = ftpClient.login(user,password);
                ftpClient.enterLocalPassiveMode();
                ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
            } catch(FTPConnectionClosedException  ioe) {
                log.error("Client being idle or some other reason causing the server to send FTP reply code 421");
                ioe.printStackTrace();
            
            } catch (IOException ioe) {
                log.error("I/O error when logging");
                ioe.printStackTrace();
            }
        return login;
    }
    
    /*
    @return FTPClient
    */
    public FTPClient getFtpClient(){
        return ftpClient;
    }
    
    /*
    @return String, name of host to ftp
    */
    public String getServer(){
        return server;
    }
    
    /*
    @return FTPFile[] list of directories in current directory
    */
    public FTPFile[] getDirectories() throws IOException {
            return ftpClient.listDirectories();
    }
    
    /*
    @return FTPFile[] list of files in current directory
    */
    public FTPFile[] getFiles() throws IOException {
            return ftpClient.listFiles();
    }
    
    /*
    @return int delay for thread
    */
    public int getRunDiscoverDelay(){
        return runDiscoverDelay;
    }
       
}
