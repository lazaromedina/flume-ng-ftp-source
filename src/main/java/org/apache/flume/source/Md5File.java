/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.flume.source;

import java.io.FileInputStream;
import java.security.MessageDigest;
import org.apache.commons.net.ftp.FTPFile;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
/**
 *
 * @author luis
 */
public class Md5File {
    private MessageDigest messageDigest;
    private FileInputStream fileFtpInput;
    private byte[] dataBytes = new byte[1024];
    private FTPFile fileFtp;
    
    public Md5File(FTPFile fileFtp){        
        this.fileFtp = fileFtp;
        try {
            messageDigest = MessageDigest.getInstance("MD5");
            fileFtpInput = new FileInputStream("/home/mortadelo/" + fileFtp.getName());
        } catch (FileNotFoundException | NoSuchAlgorithmException e){
          e.printStackTrace();
        }
    }
    
    public byte[] getMessageDigest(){
         byte[] dataBytes = new byte[1024];
        try {
            int nread = 0;
            while ((nread = fileFtpInput.read(dataBytes)) != -1) {
              messageDigest.update(dataBytes,0,nread);
            }
        } catch (IOException e){
           e.printStackTrace();
        }
        return messageDigest.digest();
    }
    
    public String toHexMessageDigest(){
        //convert the byte to hex format method 2
        StringBuffer hexString = new StringBuffer();
    	for (int i=0;i< getMessageDigest().length;i++) {
    	  hexString.append(Integer.toHexString(0xFF & getMessageDigest()[i]));
    	}
        return hexString.toString();
    }
    
    public String getMd5SumFile(){
        return toHexMessageDigest();
    }
}
