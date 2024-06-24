/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.gl.FileScpProcess.CP;
 
/**
 *
 * @author maverick
 */
public  class CDRFilesDeleteAud {
    
    
    
    private long id;

	 private String createdOn;
	private String source;
	private String operator;
	private String fileName;
	private String statusSIG1;
	private String statusSIG2;
	private String edrRecdServer;
	private String sig1Utime;
	private String sig2Utime;
	private String file_delete_time;
            private String fileDate;
                private String fileSize;
      
        private String recordSize;

    public String getFileSize() {
        return fileSize;
    }

    public void setFileSize(String fileSize) {
        this.fileSize = fileSize;
    }

    public String getRecordSize() {
        return recordSize;
    }

    public void setRecordSize(String recordSize) {
        this.recordSize = recordSize;
    }
    
            
         
        
            
            

    CDRFilesDeleteAud() {
     }

//    CDRFilesDeleteAud() {
//     }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getFileDate() {
        return fileDate;
    }

    public void setFileDate(String fileDate) {
        this.fileDate = fileDate;
    }

    public String getCreatedOn() {
        return createdOn;
    }

    public void setCreatedOn(String createdOn) {
        this.createdOn = createdOn;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getStatusSIG1() {
        return statusSIG1;
    }

    public void setStatusSIG1(String statusSIG1) {
        this.statusSIG1 = statusSIG1;
    }

    public String getStatusSIG2() {
        return statusSIG2;
    }

    public void setStatusSIG2(String statusSIG2) {
        this.statusSIG2 = statusSIG2;
    }

    public String getEdrRecdServer() {
        return edrRecdServer;
    }

    public void setEdrRecdServer(String edrRecdServer) {
        this.edrRecdServer = edrRecdServer;
    }

    public String getSig1Utime() {
        return sig1Utime;
    }

    public void setSig1Utime(String sig1Utime) {
        this.sig1Utime = sig1Utime;
    }

    public String getSig2Utime() {
        return sig2Utime;
    }

    public void setSig2Utime(String sig2Utime) {
        this.sig2Utime = sig2Utime;
    }

    public String getFile_delete_time() {
        return file_delete_time;
    }

    public void setFile_delete_time(String file_delete_time) {
        this.file_delete_time = file_delete_time;
    }

    public CDRFilesDeleteAud(long id, String createdOn, String source, String operator, String fileName, String statusSIG1, String statusSIG2, String edrRecdServer, String sig1Utime, String sig2Utime, String file_delete_time, String fileDate, String fileSize, String recordSize) {
        this.id = id;
        this.createdOn = createdOn;
        this.source = source;
        this.operator = operator;
        this.fileName = fileName;
        this.statusSIG1 = statusSIG1;
        this.statusSIG2 = statusSIG2;
        this.edrRecdServer = edrRecdServer;
        this.sig1Utime = sig1Utime;
        this.sig2Utime = sig2Utime;
        this.file_delete_time = file_delete_time;
        this.fileDate = fileDate;
        this.fileSize = fileSize;
        this.recordSize = recordSize;
    }

    @Override
    public String toString() {
        return "CDRFilesDeleteAud{" + "id=" + id + ", createdOn=" + createdOn + ", source=" + source + ", operator=" + operator + ", fileName=" + fileName + ", statusSIG1=" + statusSIG1 + ", statusSIG2=" + statusSIG2 + ", cdrRecdServer=" + edrRecdServer + ", sig1Utime=" + sig1Utime + ", sig2Utime=" + sig2Utime + ", file_delete_time=" + file_delete_time + ", fileDate=" + fileDate + ", fileSize=" + fileSize + ", recordSize=" + recordSize + '}';
    }

   

     
    
    
    
    
    

  
        
        
        
}
