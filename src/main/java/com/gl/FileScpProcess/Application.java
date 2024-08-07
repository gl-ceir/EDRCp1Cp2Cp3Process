/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.gl.FileScpProcess;

import com.gl.FileScpProcess.CP.CP1FileTransfer;
import com.gl.FileScpProcess.CP.CP2FileTransfer;
import com.gl.FileScpProcess.CP.CP3FileTransfer;
import com.gl.FileScpProcess.Config.MySQLConnection;
import com.gl.FileScpProcess.Config.PropertyReader;
import com.gl.FileScpProcess.P5Process.P5Init;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Application {

    public static PropertyReader propertyReader = new PropertyReader();

    public static String appDb;
    public static String audDb;
    public static String edrDb;
    public static void main(String[] args) {
        Connection conn = new MySQLConnection().getConnection();
        String process_parameter = args[0];
        String operator_parameter = args[1];
        String source_parameter = args[2];
        try {
            appDb = propertyReader.getConfigPropValue("appdbName");
            audDb = propertyReader.getConfigPropValue("auddbName");
            edrDb = propertyReader.getConfigPropValue("edrappdbName");
        } catch (Exception e) {
            Logger.getLogger(Application.class.getName()).log(Level.SEVERE, "Not able to fech db name " + e);
        }

        System.out.println("-----" + process_parameter + "  *************** " + operator_parameter + " #####" + source_parameter);
        if (process_parameter != null && operator_parameter != null) {
            processMethod(process_parameter, operator_parameter, source_parameter, conn);
        } else {
            System.out.println("Error: pass correct argument to run application.");
        }

        try {
            conn.close();
        } catch (SQLException ex) {
            Logger.getLogger(Application.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.exit(0);
    }

    private static void processMethod(String process_parameter, String operator_parameter, String source_parameter, Connection conn) {

        switch (process_parameter) {
            case "CP1":
                new CP1FileTransfer().cp1(operator_parameter, source_parameter, conn);
                break;
            case "CP2":
                new CP2FileTransfer().cp2(operator_parameter, source_parameter, conn);
                break;
            case "CP3":
                new CP3FileTransfer().cp3(operator_parameter, source_parameter, conn);
                break;
            case "P5":
                P5Init.start(conn,operator_parameter );
                break;
            default: System.out.println("Method doesn't exist");
                break;
        }
    }
}
