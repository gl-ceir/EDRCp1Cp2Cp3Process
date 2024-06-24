package com.gl.FileScpProcess.P5Process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;

import static com.gl.FileScpProcess.P5Process.CustomImeiPairProcess.startService;

public class LMImiePairProcess {
    static Logger log = LogManager.getLogger(LMImiePairProcess.class);

    public static void p5(Connection conn) {
        var getString = " trc_local_manufactured_device_data ";
        startService(conn, getString);
    }
}
