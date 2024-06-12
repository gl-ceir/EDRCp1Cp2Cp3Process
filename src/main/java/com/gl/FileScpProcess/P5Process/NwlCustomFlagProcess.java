package com.gl.FileScpProcess.P5Process;

import com.gl.FileScpProcess.CP.CP1FileTransfer;
import com.gl.FileScpProcess.Config.PropertyReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.Statement;


public class NwlCustomFlagProcess {
    static Logger log = LogManager.getLogger(NwlCustomFlagProcess.class);

    public static void p5(Connection conn) {
        var q = "update app.national_whitelist set tax_paid =1 where tax_paid in (0,3)  " +
                " and imei in(select imei from app.gdce_data)";
        runQuery(conn, q);
    }

    public static  void runQuery(Connection conn, String query) {
        log.info("Query : {} ", query);
        try (Statement stmt = conn.createStatement()) {
            log.info(stmt.executeUpdate(query));
        } catch (Exception e) {
            log.error("Unable to run query: " + e + "[Query] :" + query);
        }
    }


}

