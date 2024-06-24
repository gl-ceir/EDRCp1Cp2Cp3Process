/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.gl.FileScpProcess.CP;

import com.gl.FileScpProcess.AlertAudit.AlertService;
import com.gl.FileScpProcess.AlertAudit.ModuleAuditTrail;
import com.gl.FileScpProcess.Config.PropertyReader;
import com.gl.FileScpProcess.Config.StringDecryptorService;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jasypt.util.text.BasicTextEncryptor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import static java.lang.Integer.parseInt;

/**
 *
 * @author maverick
 */
public class CP2FileTransfer {

    private StringDecryptorService stringDecryptorService;

    public static PropertyReader propertyReader = new PropertyReader();
    static Logger log = LogManager.getLogger(CP2FileTransfer.class);

    int count = 0;
    int insertedKey = 0;

    public void cp2(String operator_param, String source_param, Connection conn) {
        long totalLines = 0;
        String sourceDirectory = "";
        String targetDirectory = "";
        String edrRecdServer = "";
        String start_timeStamp = "";
        String copyLocation = "";
        int timeout = 0;
        String REMOTE_HOST = "";
        String USERNAME = "";
        String PASSWORD = "";
        int REMOTE_PORT = 0;
        int SESSION_TIMEOUT = 0;
        int CHANNEL_TIMEOUT = 0;
        String REMOTE_TARGET_PATH = "";
        String SIG1 = "";
        String SIG2 = "";
        String localServiceName = "";
        String remoteServiceName = "";
        long start_time = 0;
        long executionStartTime = new Date().getTime();
        try {
            start_timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
            sourceDirectory = propertyReader.getPropValue("target_path").trim() + "/" + operator_param + "/" + source_param + "/";
            targetDirectory = propertyReader.getPropValue("remoteTargetPath").trim() + "/" + operator_param + "/" + source_param + "/";
            edrRecdServer = propertyReader.getPropValue("edrRecdServer").trim();
             copyLocation = propertyReader.getPropValue("copyLocation").trim();
            timeout = parseInt(propertyReader.getPropValue("timeout"));
            REMOTE_HOST = propertyReader.getPropValue("hostName").trim();
            USERNAME = propertyReader.getPropValue("userName").trim();
            PASSWORD = propertyReader.getPropValue("password").trim();
            REMOTE_PORT = parseInt(propertyReader.getPropValue("serverPort").trim());
            SESSION_TIMEOUT = parseInt(propertyReader.getPropValue("sessionTimeout").trim());
            CHANNEL_TIMEOUT = parseInt(propertyReader.getPropValue("channelTimeout").trim());
            REMOTE_TARGET_PATH = propertyReader.getPropValue("remoteTargetPath").trim() + "/" + operator_param + "/" + source_param + "/";
            SIG1 = propertyReader.getPropValue("server1Name").trim();
            SIG2 = propertyReader.getPropValue("server2Name").trim();
            localServiceName = propertyReader.getPropValue("localServiceName").trim();
            remoteServiceName = propertyReader.getPropValue("remoteServiceName").trim();
            insertedKey = new ModuleAuditTrail().insertModuleAudit("EDR_CP2_" + copyLocation, operator_param + "_" + source_param, conn);
            // long executionStartTime = new Date().getTime();
            log.debug(copyLocation);
//            long end_time = System.currentTimeMillis();
//            String end_timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
            //  new ModuleAuditTrail().updateModuleAudit(200, "Success", "", insertedKey, executionStartTime, count, conn, totalLines);
        } catch (Exception e) {
            log.info("tag missing in property file " + e.toString());
            // statusCode,  status,  errorMessage,  id,  executionStartTime,  numberOfRecord
        new AlertService().raiseAnAlert("alert1111", "tag missing in property file,"+e.getLocalizedMessage(), "EDR_CP2_" +operator_param+"_"+source_param, 0,conn);
            new ModuleAuditTrail().updateModuleAudit(500, "Failure", e.getLocalizedMessage(), insertedKey, executionStartTime, count, conn, totalLines);
        }

        if (localServiceName.equalsIgnoreCase(copyLocation)) {
            try {
                List<CDRFileRecords> fileDetails = new LinkedList<>();
                if (SIG1.equalsIgnoreCase(edrRecdServer)) {
                    fileDetails = findByOperatorAndSourceAndStatusSIG1AndCdrRecdServer(operator_param, source_param,
                            "INIT", edrRecdServer, conn);
                } else if (SIG2.equalsIgnoreCase(edrRecdServer)) {
                    fileDetails = findByOperatorAndSourceAndStatusSIG2AndCdrRecdServer(operator_param, source_param,
                            "INIT", edrRecdServer, conn);
                }
                log.info("Cp2 Process " + copyLocation + " ;Operator " + operator_param + " >> source " + source_param + " Copying Number of files : " + fileDetails.size());
                if (fileDetails.size() > 0) {
                    start_time = System.currentTimeMillis();
                    for (CDRFileRecords result : fileDetails) {
                        String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
                        String destinationPath = targetDirectory + result.getFileName();
                        log.debug("copy file to destinationPath:" + destinationPath);
                        Files.copy(Paths.get(sourceDirectory + result.getFileName()),
                                Paths.get(destinationPath), StandardCopyOption.REPLACE_EXISTING);
                        log.debug("File copied successfully to destination path " + destinationPath);
                        count += 1;
                        // updating file status
                        if (SIG1.equalsIgnoreCase(edrRecdServer)) {
                            log.debug("LOCAL : rec. server : " + edrRecdServer + " updating status to DONE");
                            modifyFileStatus("DONE", result.getStatusSIG2(), timeStamp,
                                    result.getSig2Utime(), result.getId(), SIG1, conn);
                        } else if (SIG2.equalsIgnoreCase(edrRecdServer)) {
                            log.debug("LOCAL : rec. server : " + edrRecdServer + " updating status to DONE");
                            modifyFileStatus(result.getStatusSIG1(), "DONE", result.getSig1Utime(),
                                    timeStamp, result.getId(), SIG2, conn);
                        }

                    }
                }
                long end_time = System.currentTimeMillis();
                String end_timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
                log.info("FINAL COPY PROCESS :: " + copyLocation + " start time : " + start_timeStamp + " >> end time : " + end_timeStamp
                        + " >> file transferred : " + count + " >> total time taken :"
                        + ((end_time - start_time) / 1000) / 60 + "minutes and " + ((end_time - start_time) / 1000) % 60
                        + " seconds >>" + " operator :" + operator_param + " >> source :" + source_param
                        + " ");
                new ModuleAuditTrail().updateModuleAudit(200, "Success", "", insertedKey, executionStartTime, count, conn, totalLines);
            } catch (Exception e) {
                log.info("unable to copy files at local machine. " + e.toString());
                // statusCode,  status,  errorMessage,  id,  executionStartTime,  numberOfRecord
              new AlertService().raiseAnAlert("alert1111", "unable to copy files at local,"+e.getLocalizedMessage(), "CP2_"+operator_param+"_"+source_param , 0,conn);
                new ModuleAuditTrail().updateModuleAudit(500, "Failure", e.getLocalizedMessage(), insertedKey, 0, count, conn, totalLines);
            }
        }

        if (remoteServiceName.equalsIgnoreCase(copyLocation)) {
            List<CDRFileRecords> fileDetails = new LinkedList<>();
            try {
                if (SIG1.equalsIgnoreCase(edrRecdServer)) {
                    fileDetails = findByOperatorAndSourceAndStatusSIG2AndCdrRecdServer(operator_param, source_param, "INIT", edrRecdServer, conn);
                } else if (SIG2.equalsIgnoreCase(edrRecdServer)) {
                    fileDetails = findByOperatorAndSourceAndStatusSIG1AndCdrRecdServer(operator_param, source_param, "INIT", edrRecdServer, conn);
                }
                log.info("Cp2 Process  Remote " + copyLocation + "! Operator " + operator_param + " >> source " + source_param + " Copying Number of files : " + fileDetails.size());
                if (fileDetails.size() > 0) {
                    boolean isServerUtilityAlive = isSocketAlive(REMOTE_HOST, REMOTE_PORT, timeout);
                    if (isServerUtilityAlive == true) {
                        Session jschSession = null;
                        start_time = System.currentTimeMillis();
//                    try {
                        JSch jsch = new JSch();
                        jschSession = jsch.getSession(USERNAME, REMOTE_HOST, REMOTE_PORT);  // authenticate using password
                        jschSession.setPassword(decryptor(PASSWORD));
                        jschSession.setConfig("StrictHostKeyChecking", "no"); // 10 seconds session timeout
                        jschSession.connect(SESSION_TIMEOUT);
                        //    log.info("Cp2 Process  Remote with No Pass : jschSession : " + jschSession);
                        Channel sftp = jschSession.openChannel("sftp");  // 5 seconds timeout
                        sftp.connect(CHANNEL_TIMEOUT);
                        log.info("Cp2 Process  Remote with sftp : " + sftp);
                        ChannelSftp channelSftp = null;
                        for (CDRFileRecords result : fileDetails) {
                            String timeStamp_during_remote = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
                            channelSftp = (ChannelSftp) sftp;   // transfer file from local to remote server
                            channelSftp.put(sourceDirectory + result.getFileName(), REMOTE_TARGET_PATH + result.getFileName());
                            log.info("file " + result.getFileName() + " copied to server " + REMOTE_HOST);
                            count += 1;
                            // download file from remote server to local   channelSftp.get(remoteFile, localFile); // updating file status
                            if (SIG1.equalsIgnoreCase(edrRecdServer)) {
                                modifyFileStatus(result.getStatusSIG1(), "DONE", result.getSig1Utime(),
                                        timeStamp_during_remote, result.getId(), SIG1, conn);
                                log.debug("REMOTE : rec. server : " + edrRecdServer + " updating status to DONE");
                            } else if (SIG2.equalsIgnoreCase(edrRecdServer)) {
                                modifyFileStatus("DONE", result.getStatusSIG2(), timeStamp_during_remote,
                                        result.getSig2Utime(), result.getId(), SIG1, conn);
                                log.debug("REMOTE file " + result.getFileName() + " copied to server " + REMOTE_HOST + " : rec. server : " + edrRecdServer + " updated status to DONE");
                            }
                            log.info("CP2_Remote File " + result.getFileName() + " copied to server " + REMOTE_HOST + " :  rec. server : " + edrRecdServer + " updated status to DONE");
                        }
                        channelSftp.exit();
                        if (jschSession != null) {
                            jschSession.disconnect();
                            log.info("connection closed with hostname " + REMOTE_HOST);
                        }
                    } else {
                        log.warn("unable to create conncetion between remote machine. ");
                         new AlertService().raiseAnAlert("alert1111", "unable to copy between remote machine ,", "EDR_CP2_Remote_"+operator_param+"_"+source_param , 0,conn);
                        new ModuleAuditTrail().updateModuleAudit(500, "Failure", "unable to establish connection", insertedKey, 0, count, conn, totalLines);
                        System.exit(0);
                    }
                }
                long end_time = System.currentTimeMillis();
                String end_timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
                log.info("FINAL COPY REMOTE PROCESS :: " + copyLocation + " start time : " + start_timeStamp + " >> end time : " + end_timeStamp
                        + " >> file transferred : " + count + " >> total time taken :"
                        + ((end_time - start_time) / 1000) / 60 + "minutes and " + ((end_time - start_time) / 1000) % 60
                        + " seconds >>" + " operator :" + operator_param + " >> source :" + source_param
                        + "  : File Count " + count);
                new ModuleAuditTrail().updateModuleAudit(200, "Success", "", insertedKey, executionStartTime, count, conn, totalLines);
            } catch (Exception e) {
                log.info("unable to copy files between remote machine. " + e.toString());
                new AlertService().raiseAnAlert("alert1111", "unable to copy files between remote machine,"+e.getLocalizedMessage(), "CP2_Remote_"+operator_param+"_"+source_param , 0,conn);
                new ModuleAuditTrail().updateModuleAudit(500, "Failure", e.getLocalizedMessage(), insertedKey, executionStartTime, count, conn, totalLines);
            }
        }
    }

    public boolean isSocketAlive(String hostName, int port, int timeout) {
        boolean isAlive = false;
        SocketAddress socketAddress = new InetSocketAddress(hostName, port);
        Socket socket = new Socket();
        try {
            socket.connect(socketAddress, timeout);
            socket.close();
            isAlive = true;
            log.info("hostname " + hostName + " is listening on port" + port);
        } catch (SocketTimeoutException exception) {
            log.info("SocketTimeoutException " + hostName + ":" + port + ". " + exception.getMessage());
        } catch (IOException exception) {
            log.info("IOException - Unable to connect to " + hostName + ":" + port + ". " + exception.getMessage());
        }
        return isAlive;
    }

    public String decryptor(String encryptedText) {
        BasicTextEncryptor encryptor = new BasicTextEncryptor();
        encryptor.setPassword(System.getenv("JASYPT_ENCRYPTOR_PASSWORD"));
        return encryptor.decrypt(encryptedText);
    }

    private List<CDRFileRecords> findByOperatorAndSourceAndStatusSIG1AndCdrRecdServer(String operator_param,
            String source_param, String init, String cdrRecdServer, Connection conn) {
        String stsSig = "STATUS_ETL1";
        Statement stmt = null;
        ResultSet rs = null;
        String query = null;
        List<CDRFileRecords> CDRFileRecordss = new LinkedList<>();
        try {
            query = "select id , source , operator, FILE_NAME ,STATUS_ETL1 ,STATUS_ETL2,EDR_RECD_SERVER,STS_ETL1_UTIME, STS_ETL2_UTIME , FILE_DATE  ,file_size, record_size" +
                    "  from "
                    + propertyReader.getConfigPropValue("edrappdbName") + ".cdr_file_received_detail where"
                    + " operator = '" + operator_param + "' and  source = '" + source_param + "' and   " + stsSig
                    + "  = '" + init + "' and EDR_RECD_SERVER  = '" + cdrRecdServer + "'  ";

            log.info("Query:->" + query);
            stmt = conn.createStatement();
            rs = stmt.executeQuery(query);
            while (rs.next()) {
                CDRFileRecordss.add(new CDRFileRecords(rs.getLong("id"),
                        rs.getString("source"),
                        rs.getString("operator"), rs.getString("file_Name"),
                        rs.getString("STATUS_ETL1"), rs.getString("STATUS_ETL2"),
                        rs.getString("EDR_RECD_SERVER"), rs.getString("STS_ETL1_UTIME"), rs.getString("STS_ETL2_UTIME"),
                        rs.getString("FILE_DATE"), rs.getString("file_size"), rs.getString("record_size")));
            }
        } catch (Exception e) {
            log.error("" + e);
            new AlertService().raiseAnAlert("alert1111", "Failed To get details from cdr_file_received_detail ," + e.getLocalizedMessage(), "CP1_" + operator_param + "_" + source_param, 0, conn);

        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                log.error("" + e);
            }
        }
        return CDRFileRecordss;
    }

    private List<CDRFileRecords> findByOperatorAndSourceAndStatusSIG2AndCdrRecdServer(String operator_param,
            String source_param, String init, String cdrRecdServer, Connection conn) {
        String stsSig = "STATUS_ETL2";
        Statement stmt = null;
        ResultSet rs = null;
        String query = null;
        List<CDRFileRecords> CDRFileRecordss = new LinkedList<>();
        try {
            query = "select id , source , operator, FILE_NAME ,STATUS_ETL1 ,STATUS_ETL2,EDR_RECD_SERVER,STS_ETL1_UTIME, STS_ETL2_UTIME ,FILE_DATE ,file_size, record_size  from "
                    + propertyReader.getConfigPropValue("edrappdbName") + ".cdr_file_received_detail where"
                    + " operator = '" + operator_param + "' and  source = '" + source_param + "' and   " + stsSig
                    + "  = '" + init + "' and EDR_RECD_SERVER  = '" + cdrRecdServer + "'  ";

            log.info("Query:>" + query);
            stmt = conn.createStatement();
            rs = stmt.executeQuery(query);
            while (rs.next()) {
                CDRFileRecordss.add(new CDRFileRecords(rs.getLong("id"),
                        rs.getString("source"),
                        rs.getString("operator"), rs.getString("file_Name"),
                        rs.getString("STATUS_ETL1"), rs.getString("STATUS_ETL2"),
                        rs.getString("EDR_RECD_SERVER"), rs.getString("STS_ETL1_UTIME"), rs.getString("STS_ETL2_UTIME"),
                        rs.getString("FILE_DATE"), rs.getString("file_size"), rs.getString("record_size")));
            }
        } catch (Exception e) {
            log.error("" + e);
            new AlertService().raiseAnAlert("alert1111", "Failed To get details from cdr_file_received_detail ," + e.getLocalizedMessage(), "CP1_" + operator_param + "_" + source_param, 0, conn);

        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                log.error("" + e);
            }
        }
        return CDRFileRecordss;
    }

    private void modifyFileStatus(String statusSIG1, String statusSIG2, String sig1Utime, String sig2Utime, long id,
            String server, Connection conn) {
        try {
            String query = null;
            // commented as both  are equal. 23jun23
//            if (server.equals(SIG1)) {
//                query = "update " + propertyReader.getConfigPropValue("appdbName")
//                        + ".cdr_file_received_detail  set STATUS_SI G1 = '" + statusSIG1 + "' ,STATUS_S IG2 = '"
//                        + statusSIG2 + "', STS_SI G1_UTIME='" + sig1Utime + "' , STS_S IG2_UTIME='" + sig2Utime
//                        + "' where ID = " + id + "";
//            } else {

            if (conn.toString().contains("oracle")) {

                sig1Utime = "TO_DATE('" + sig1Utime + "','YYYY-MM-DD HH24:MI:SS')";
                sig2Utime = "TO_DATE('" + sig2Utime + "','YYYY-MM-DD HH24:MI:SS')";
                query = "update " + propertyReader.getConfigPropValue("edrappdbName")
                        + "cdr_file_received_detail  set STATUS_ETL1 = '" + statusSIG1 + "' ,STATUS_ETL2 = '"
                        + statusSIG2 + "', STS_ETL1_UTIME=" + sig1Utime + " , STS_ETL2_UTIME=" + sig2Utime
                        + " where ID = " + id + "";
            } else {

                query = "update " + propertyReader.getConfigPropValue("edrappdbName")
                        + ".cdr_file_received_detail  set STATUS_ETL1 = '" + statusSIG1 + "' ,STATUS_ETL2 = '"
                        + statusSIG2 + "', STS_ETL1_UTIME='" + sig1Utime + "' , STS_ETL2_UTIME='" + sig2Utime
                        + "' where ID = " + id + "";

            }
            Statement stmt = conn.createStatement();
            log.info(query);
            stmt.executeUpdate(query);
            stmt.close();
        } catch (Exception e) {           
            log.error("Error while updating status for " + id, e);
      new AlertService().raiseAnAlert("alert1111", "Failed while updating status for "+id +"  ;  "+e.getLocalizedMessage(), "CP2" , 0,conn);

        }
    }
}
