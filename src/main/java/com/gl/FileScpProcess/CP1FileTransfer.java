/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.gl.FileScpProcess;

import com.gl.FileScpProcess.AlertService;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.gl.FileScpProcess.ModuleAuditTrail.defaultDateNow;

public class CP1FileTransfer {

    public static PropertyReader propertyReader = new PropertyReader();
    static Logger log = LogManager.getLogger(CP1FileTransfer.class);

    public void cp1(String operator_param, String source_param, Connection conn) {
        int insertedKey = 0;
        String sourceDirectory = "";
        String targetDirectory = "";
        String cdrRecdServer = "";
        String extension = "";
        try {
            sourceDirectory = propertyReader.getPropValue("source_path").trim() + "/" + operator_param + "/" + source_param + "/";
            targetDirectory = propertyReader.getPropValue("target_path").trim() + "/" + operator_param + "/" + source_param + "/";
            cdrRecdServer = propertyReader.getPropValue("edrRecdServer").trim();
            extension = propertyReader.getPropValue("EXTENSION").trim();

            log.info("target_path-------" + targetDirectory);
        } catch (IOException ex) {
            log.error("Error Properties not found", ex);
              new AlertService().raiseAnAlert ("alert1111", "No File Found", "CP1 " + operator_param + "_" + source_param, 0,conn);
        }
        String start_timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
        insertedKey = new ModuleAuditTrail().insertModuleAudit("EDR_CP1", operator_param + "_" + source_param, conn);
        long executionStartTime = new Date().getTime();
        long start_time = 0;
        start_time = System.currentTimeMillis();
        int count = 0;
        int fileCount = 0;
        File[] files = new File(sourceDirectory).listFiles();
        if (files == null) {
            new ModuleAuditTrail().updateModuleAudit(200, "Success", "", insertedKey, executionStartTime, count, conn, fileCount);
            System.exit(0);
        }
        Arrays.sort(files, Comparator.comparingLong(File::lastModified));
        log.info("Operator " + operator_param + " >> source " + source_param + " :Total  Number of files : " + files.length + " Skiping 1 Lastest File ");
        long totalLines = 0;
        try {
            for (int i = 0; i < files.length - 1; i++) {
                File file = files[i];
                if (file.isFile() && !file.getName().endsWith(extension)) {
                    long fileSize = file.length();
                    log.debug("File Size " + fileSize);
                    long lines = 0;
                    try {
                        long val = Files.size(new File(sourceDirectory + file.getName()).toPath());
                        log.debug("File Size value " + val);
                        lines = Files.lines(Paths.get(sourceDirectory + file.getName())).parallel().count();
                        log.debug("Lines in File  " + lines);
                        totalLines = lines + totalLines;
                    } catch (IOException ex) {
                        log.error("Error ", ex);
                    }
                    String destinationPath = targetDirectory + file.getName();
                    log.debug("moving file to destinationPath:" + destinationPath);
                    try {
                        Path temp = Files.move(Paths.get(sourceDirectory + file.getName()), Paths.get(destinationPath));
                        log.debug("File moved successfully to destination path " + destinationPath);
                        count = count + 1;
                        saveDatainDb(conn, source_param.trim(), operator_param, file.getName(), cdrRecdServer, fileSize, start_timeStamp, lines - 1);
                    } catch (IOException e) {
                        log.error("Failed to move the file " + file.getName() + " due to reason : " + e.toString());
                        //   new AlertService().raiseAnAlert("alert1111", e.getLocalizedMessage(), "CP1", 0);
                    }
                } else {
                    log.warn("File is ending with extenstion inp,  File Name is  " + file.getName());
                }
            }
            new ModuleAuditTrail().updateModuleAudit(200, "Success", "", insertedKey, executionStartTime, count, conn, totalLines);
        } catch (Exception ex) {
            log.error("Error ", ex);
          new AlertService().raiseAnAlert("alert1111", ex.getLocalizedMessage(), "CP1_"+ operator_param + "_" + source_param, 0,conn);
            new ModuleAuditTrail().updateModuleAudit(500, "Failure", ex.getLocalizedMessage(), insertedKey, executionStartTime, count, conn, totalLines);
        }
        long end_time = System.currentTimeMillis();
        String end_timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
        log.info("FINAL CP1 MOVE PROCESS ::  start time : " + start_timeStamp + " >> end time : " + end_timeStamp + " >> file transferred : " + count + " >> total time taken :" + ((end_time - start_time) / 1000) / 60 + "minutes and " + ((end_time - start_time) / 1000) % 60 + " seconds >>" + " operator :" + operator_param + " >> source :" + source_param + " >> Record Count " + totalLines);
    }

    static void saveDatainDb(Connection conn, String source, String operator, String name, String cdrRecdServer, long filesize, String start_timeStamp, long lines) {
        try (Statement stmt = conn.createStatement()) {
            String dateFunction =  defaultDateNow(conn.toString().contains("oracle"));
            String query = "insert into " + propertyReader.getConfigPropValue("edrappdbName") + ".edr_file_received_detail ( CREATED_ON ,SOURCE ,OPERATOR, FILE_NAME , EDR_RECD_SERVER , STATUS_SIG1 ,STATUS_SIG2  , FILE_DATE , file_size, record_size) "
                    + " values( " + dateFunction + " , '" + source + "' , '" + operator + "' ,  '" + name + "' ,   '" + cdrRecdServer + "' , 'INIT' , 'INIT'   ,current_timestamp,    '" + filesize + "' ,   '" + lines + "' ) ";
            log.info(query);
            stmt.executeUpdate(query);
        } catch (Exception e) {
            log.error("Failed  " + e);
          new AlertService().raiseAnAlert("alert1111", "Failed To save in edr_file_received_detail ,"+e.getLocalizedMessage(), "CP1_"+ operator + "_" + source, 0,conn);

        }
    }

}

//    public static void main123(String[] args) {kl
//
//        String source_directory = "";
//        String target_directory = "";
//        try {
//            source_directory = propertyReader.getPropValue("source_directory").trim();
//            target_directory = propertyReader.getPropValue("target_directory").trim();
//        } catch (IOException ex) {
//            java.util.logging.Logger.getLogger(CP1FileTransfer.class.getName()).log(Level.SEVERE, null, ex);
//        }
//
//        log.info("source_directory : " + source_directory);
//        log.info("target_directory : " + target_directory);
//
//        String[] arr = source_directory.split("/");
//        String source = arr[arr.length - 1];
//        String operator = arr[arr.length - 2];
//        File[] files = new File(source_directory).listFiles();
//        log.info("Number of files : " + files.length);
//        for (File file : files) {
////            CDRFileRecords entity = new CDRFileRecords();
//            if (file.isFile()) {
//                Connection conn = (Connection) new com.gl.FileScpProcess.MySQLConnection().getConnection();
//                String destinationPath = target_directory + file.getName();
//                log.info("moving file to destinationPath:" + destinationPath);
//                Path temp;
//                try {
//                    temp = Files.move(Paths.get(source_directory + file.getName()), Paths.get(destinationPath));
//                    log.info("File moved successfully to destination path " + destinationPath);
//                    saveDatainDb(conn, source, operator, file.getName());
//                } catch (IOException e) {
//                    log.info("Failed to move the file");
//                    e.printStackTrace();
//                }
//
//            }
//        }
//    }
