package com.gl.FileScpProcess;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author maverick
 */
public class ModuleAuditTrail {

    public static PropertyReader propertyReader = new PropertyReader();
    static Logger log = LogManager.getLogger(ModuleAuditTrail.class);

    public int insertModuleAudit(String featureName, String processName, Connection conn) {
        String query = "";
        int generatedKey = 0;
        try {
            query = " insert into  " + propertyReader.getConfigPropValue("auddbName") + ".modules_audit_trail "
                    + "(status_code,status,feature_name,"
                    + "info, count2,action,"
                    + "server_name,execution_time,module_name,failure_count) "
                    + "values('201','Initial', '" + featureName + "', '" + processName + "' ,'0','Insert', '"
                    + propertyReader.getConfigPropValue("serverName") + "','0','EDR','0')";

        log.info(query);
        // For Mysql
        // try (PreparedStatement ps = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);) {

            PreparedStatement ps = null;
            if (conn.toString().contains("oracle")) {
                ps = conn.prepareStatement(query, new String[]{"ID"});
            } else {
                ps = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            }
            log.debug("Going to execute  ");
            ps.execute();
            log.debug("Going for getGenerated key  ");
            ResultSet rs = ps.getGeneratedKeys();
            if (rs.next()) {
                generatedKey = rs.getInt(1);
            }
            log.info("Inserted record's ID: " + generatedKey);
            rs.close();
        } catch (Exception e) {
            log.error("Failed  " + e);

        }
        return generatedKey;
    }

    public void updateModuleAudit(int statusCode, String status, String errorMessage, int id, long executionStartTime, int numberOfRecord, Connection conn, long totalLineCount) {
        String exec_time = " TIMEDIFF(now() ,created_on) ";
        if (conn.toString().contains("oracle")) {
            long milliseconds = (new Date().getTime()) - executionStartTime;
            String executionFinishTiime = (((milliseconds / 1000) / 60) / 60) + ":" + (((milliseconds / 1000) / 60) % 60) + ":" + ((milliseconds / 1000) % 60);
            exec_time = " '" + executionFinishTiime + "' ";
        }
        // for Oracle
        //   String executionFinishTiime = (((milliseconds / 1000) / 60) / 60) + ":" + (((milliseconds / 1000) / 60) % 60) + ":" + ((milliseconds / 1000) % 60);
        // for Mysql execution_time = TIMEDIFF(now() ,created_on)

        try (Statement stmt = conn.createStatement()) {
            String query = "update   " + propertyReader.getConfigPropValue("auddbName") + ".modules_audit_trail " +
                    "set status_code='" + statusCode + "',status='" + status + "',error_message='" + errorMessage + "', count='" + numberOfRecord + "',"
                    + "action='update',  execution_time = " + exec_time + " , modified_on = " + defaultDateNow(conn) + " ,      count2='" + totalLineCount + "'  where  id = " + id;
            log.info(query);
            stmt.executeUpdate(query);
        } catch (Exception e) {
            log.info("Failed  " + e);
        }
    }

    public static String defaultDateNow(Connection conn) {
        if (conn.toString().contains("oracle")) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String val = sdf.format(new Date());
            String date = "TO_DATE('" + val + "','YYYY-MM-DD HH24:MI:SS')"; // commented by sharad
            return date;
        } else {
            return "now()";
        }
    }


    public static String defaultDateNow(boolean isOracle) {
        if (isOracle) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String val = sdf.format(new Date());
            String date = "TO_DATE('" + val + "','YYYY-MM-DD HH24:MI:SS')";  // commented by sharad
            return date;
        } else {
            return "now()";
        }
    }


}
