package com.gl.FileScpProcess.P5Process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;

public class P5Init {

    static Logger log = LogManager.getLogger(P5Init.class);

    public static void start(Connection c, String process) {
        switch (process) {
            case "RegisterIMEIUpdate":
                CustomImeiPairProcess.p5(c);
                break;
            case "EdrUpdateMsisdn":
                EdrUpdateMsisdnProcess.p5(c);
                break;
            case "LMImeiPair":
                LMImiePairProcess.p5(c);
                break;

            case "LMNwl":
                NwlLMFlagProcess.p5(c);
                break;
            case "LMNwlUpdate":
                NwlLmUpdateProcess.p5(c);
                break;
            case "CustomNwlUpdate":
                NwlCustomUpdateProcess.p5(c);
                break;
            default:
                log.info("Process doesn't exist");
                break;
        }
    }
}
