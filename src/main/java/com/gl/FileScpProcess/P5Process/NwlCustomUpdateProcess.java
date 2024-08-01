package com.gl.FileScpProcess.P5Process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;

import static com.gl.FileScpProcess.P5Process.QueryExecuter.runQuery;


public class NwlCustomUpdateProcess {
    static Logger log = LogManager.getLogger(NwlCustomUpdateProcess.class);

    public static void p5(Connection conn) {
        var q = "update app.national_whitelist set tax_paid =1 where  " +
                "  imei in(select imei from app.gdce_data)";
        runQuery(conn,q);
    }

}

