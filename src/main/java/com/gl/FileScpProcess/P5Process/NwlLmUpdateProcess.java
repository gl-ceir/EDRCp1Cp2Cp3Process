package com.gl.FileScpProcess.P5Process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;

import static com.gl.FileScpProcess.P5Process.NwlCustomFlagProcess.runQuery;

public class NwlLmUpdateProcess {
    static Logger log = LogManager.getLogger(NwlLmUpdateProcess.class);

    public static void p5(Connection conn) {
        var q = "update app.national_whitelist set tax_paid =2 where " +
                "  imei in(select imei from app.trc_local_manufactured_device_data)";
        runQuery(conn, q);
    }
}
