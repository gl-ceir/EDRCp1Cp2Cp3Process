package com.gl.FileScpProcess.P5Process;

import com.gl.FileScpProcess.AlertAudit.AlertService;
import com.gl.FileScpProcess.AlertAudit.ModuleAuditTrail;
import com.gl.FileScpProcess.CP.CP1FileTransfer;
import com.gl.FileScpProcess.Config.PropertyReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.Statement;


public class NwlCustomFlagProcess {
    static Logger log = LogManager.getLogger(NwlCustomFlagProcess.class);

    public static void p5(Connection conn) {
        var q = "update app.national_whitelist set gdce_imei_status =1, gdce_modified_time =CURRENT_TIMESTAMP where gdce_imei_status in (0,3)  " +
                " and imei in(select imei from app.gdce_data where created_on >= ( select imei from app.gdce_data where created_on >= ( select IFNULL(value, '') from sys_param where tag ='gdce_register_imei_update_last_run_time' )  )   )";
        runQuery(conn, q);
    }

    public static  void runQuery(Connection conn, String query) {
        log.info("Query : {} ", query);
        try (Statement stmt = conn.createStatement()) {
            log.info(stmt.executeUpdate(query));
        } catch (Exception e) {
           var lastMethodName= Thread.currentThread().getStackTrace()[2].getMethodName();
            log.error(lastMethodName +" : Unable to run query: " + e.getLocalizedMessage() + " [Query] :" + query);
            new AlertService().raiseAnAlert("alert1769",e.getLocalizedMessage().replaceAll("'", " ")   , "RegisterIMEIUpdate ", 0,conn);
        }
    }

}

