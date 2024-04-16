/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.gl.FileScpProcess;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author maverick
 */
public class CP3FileTransfer {

    public static PropertyReader propertyReader = new PropertyReader();
    static Logger log = LogManager.getLogger(CP3FileTransfer.class);

    public void cp3(String operator_param, String source_param, Connection conn) {
        long executionStartTime = new Date().getTime();
        int insertedKey = 0;
        try {
            String edrRecdServer = "";
            edrRecdServer = propertyReader.getPropValue("edrRecdServer").trim();
            String start_timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
            int count = 0;
            long start_time = 0;
            insertedKey = new ModuleAuditTrail().insertModuleAudit("EDR_CP3", operator_param + "_" + source_param, conn);
            String delete_file_from_dir = propertyReader.getPropValue("target_path").trim() + "/" + operator_param + "/" + source_param + "/";
            log.info("file to be delete from " + delete_file_from_dir);
            List<CDRFileRecords> fileDetails = findByOperatorAndStatusSIG1AndStatusSIG2(operator_param, "DONE", "DONE", conn, source_param);
            log.info("CP3 Operator " + operator_param + " >> source " + source_param + " Number of files to Delete : " + fileDetails.size());
            int zro = 0;
            if (fileDetails.size() > zro) {
                start_time = System.currentTimeMillis();
                log.info("inside deletion process ");
//                for (int i = 0; i < optional.size(); i++) {//   String filName = optional.get(i).getCdrRecdServer();//  log.info("inside deletion process 99 " + filName);//                }
                for (CDRFileRecords result : fileDetails) {
                    try {
                        File file = new File(delete_file_from_dir + result.getFileName());
                        if (file.exists()) {
                            Path path = Paths.get(delete_file_from_dir + result.getFileName());
                            Files.delete(path);
                            count = count + 1;
                            try {
                                CDRFilesDeleteAud cdrFilesDeleteAud = new CDRFilesDeleteAud();
                                cdrFilesDeleteAud.setSource(source_param);
                                cdrFilesDeleteAud.setOperator(operator_param);
                                cdrFilesDeleteAud.setFileName(result.getFileName());
                                cdrFilesDeleteAud.setStatusSIG1("DONE");
                                cdrFilesDeleteAud.setStatusSIG2("DONE");
                                cdrFilesDeleteAud.setEdrRecdServer(edrRecdServer);
                                cdrFilesDeleteAud.setSig1Utime(result.getSig1Utime());
                                cdrFilesDeleteAud.setSig2Utime(result.getSig2Utime());
                                cdrFilesDeleteAud.setFileDate(result.getFileDate());
                                cdrFilesDeleteAud.setFileSize(result.getFileSize());
                                cdrFilesDeleteAud.setRecordSize(result.getRecordSize());
                                savedetails(cdrFilesDeleteAud, conn);
                                deleteByIdDetails(result.getId(), conn);
                                log.info("File removed : " + delete_file_from_dir + result.getFileName() + "Saved in aud table and deleted from  edr_file_received_detail");
                            } catch (Exception e) {
                                log.error("unusual happend during insertion in edr_file_delete_aud table : " + e.toString());
                            }
                        } else {
                            log.error("file does not exist");
                        }
                    } catch (Exception e) {
                        log.error(" oops updation failed due to reason ;Failed to remove file from path " + delete_file_from_dir + "due to reason" + e.toString());
                        new AlertService().raiseAnAlert("alert1111", "Failed to remove file from path " + delete_file_from_dir + "due to reason" + e.getLocalizedMessage(), "CP3_" + operator_param + "_" + source_param, 0,conn);
                    }
                }
                long end_time = System.currentTimeMillis();
                String end_timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
                log.info("FINAL CP3 DELETE PROCESS ::  start time : " + start_timeStamp + " >> end time : " + end_timeStamp + " >> file transferred : " + count + " >> total time taken :" + ((end_time - start_time) / 1000) / 60 + "minutes and " + ((end_time - start_time) / 1000) % 60 + " seconds >>" + " operator :" + operator_param + " >> source :" + source_param + " >> file transferred per second");
            }
            new ModuleAuditTrail().updateModuleAudit(200, "Success", "", insertedKey, executionStartTime, count, conn, 0);
        } catch (Exception e) {
            log.error(e);
            new AlertService().raiseAnAlert("alert1111", "Failed to remove file from path " + e.getLocalizedMessage(), "CP3_" + operator_param + "_" + source_param, 0,conn);
            new ModuleAuditTrail().updateModuleAudit(500, "Failure", e.getLocalizedMessage(), insertedKey, executionStartTime, 0, conn, 0);
        }
    }

    private List<CDRFileRecords> findByOperatorAndStatusSIG1AndStatusSIG2(String operator_param, String StatusSIG1, String StatusSIG2, Connection conn, String source) {

        Statement stmt = null;
        ResultSet rs = null;
        String query = null;

        List<CDRFileRecords> CDRFileRecordss = new LinkedList<>();
        try {
            query = "select id , source , operator, FILE_NAME ,STATUS_SIG1 ,STATUS_SIG2,EDR_RECD_SERVER,STS_SIG1_UTIME, STS_SIG2_UTIME , file_date  , file_size , record_size from  " + propertyReader.getConfigPropValue("edrappdbName") + ".edr_file_received_detail where"
                    + " operator = '" + operator_param + "'    and     source  = '" + source + "' and     STATUS_SIG1  = '" + StatusSIG1 + "' and  STATUS_SIG2  = '" + StatusSIG2 + "'  ";
            log.info("Query:->" + query);
            stmt = conn.createStatement();
            rs = stmt.executeQuery(query);
            while (rs.next()) {
                CDRFileRecordss.add(new CDRFileRecords(rs.getLong("id"),
                        rs.getString("source"),
                        rs.getString("operator"), rs.getString("file_Name"),
                        rs.getString("STATUS_SIG1"), rs.getString("STATUS_SIG2"),
                        rs.getString("EDR_RECD_SERVER"), rs.getString("STS_SIG1_UTIME"), rs.getString("STS_SIG2_UTIME"), rs.getString("file_date"), rs.getString("file_size"), rs.getString("record_size")
                ));
            }
        } catch (Exception e) {
            log.error("" + e);
            new AlertService().raiseAnAlert("alert1111", "" + e.getLocalizedMessage(), "EDR_CP3_" + operator_param + "_" + source, 0,conn);

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

    private void savedetails(CDRFilesDeleteAud cdrFilesDeleteAud, Connection conn) {
        String query = null;
        try {
            query = "insert into  " + propertyReader.getConfigPropValue("auddbName") + ".edr_file_delete_aud ( CREATED_ON ,SOURCE ,OPERATOR, FILE_NAME,STATUS_SIG1 ,STATUS_SIG2 ,EDR_RECD_SERVER ,FILE_DELETE_TIME   ,file_date ,file_size  , record_size ) " //  ,rs.getString("")      , rs.getString("")
                    + " values( current_timestamp , '" + cdrFilesDeleteAud.getSource() + "' , '" + cdrFilesDeleteAud.getOperator() + "' ,  '" + cdrFilesDeleteAud.getFileName() + "' , '" + cdrFilesDeleteAud.getStatusSIG1() + "' , '" + cdrFilesDeleteAud.getStatusSIG2() + "' , '" + cdrFilesDeleteAud.getEdrRecdServer() + "' , current_timestamp  , '" + cdrFilesDeleteAud.getFileDate() + "'  ,   '" + cdrFilesDeleteAud.getFileSize() + "'  ,  '" + cdrFilesDeleteAud.getRecordSize() + "'         )";
            log.info("Query:--=>" + query);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(query);
            }
        } catch (Exception e) {
            log.info("Failed  " + e + "[query] " + query);
        }
    }

    private void deleteByIdDetails(long id, Connection conn) {
        String query = null;
        try {
            query = " delete from  " + propertyReader.getConfigPropValue("edrappdbName") + ".edr_file_received_detail  where id = " + id;
            log.info("Query:->" + query);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(query);
            }
        } catch (Exception e) {
            log.info("D eleteByIdDetails Failed  " + e + "[Query]" + query);
            new AlertService().raiseAnAlert("alert1111", "Unable to delete from edr_file_received_detail via id " + id + "  " + e.getLocalizedMessage(), "CP3", 0,conn);

        }
    }
}
