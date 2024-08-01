package com.gl.FileScpProcess.P5Process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;

import static com.gl.FileScpProcess.P5Process.QueryExecuter.runQuery;

public class NwlLMFlagProcess {
    static Logger log = LogManager.getLogger(NwlLMFlagProcess.class);

    public static void p5(Connection conn) {
        var q = "update app.national_whitelist set tax_paid =2 where tax_paid not in  (1,2)  " +
                " and imei in(select imei from app.trc_local_manufactured_device_data)";
        runQuery(conn, q);
    }
}
