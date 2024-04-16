/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.gl.FileScpProcess;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Application {

    public static void main(String[] args) {
        System.out.println("start main method");
        Connection conn = new com.gl.FileScpProcess.MySQLConnection().getConnection();
        String process_parameter = args[0];
        String operator_parameter = args[1];
        String source_parameter = args[2];
        System.out.println("-----" + process_parameter + "  *************** " + operator_parameter + " #####" + source_parameter);
        if (process_parameter != null && operator_parameter != null && source_parameter != null) {
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
                new com.gl.FileScpProcess.CP1FileTransfer().cp1(operator_parameter, source_parameter, conn);
                break;
            case "CP2":
                new com.gl.FileScpProcess.CP2FileTransfer().cp2(operator_parameter, source_parameter, conn);
                break;
            case "CP3":
                new com.gl.FileScpProcess.CP3FileTransfer().cp3(operator_parameter, source_parameter, conn);
                break;
            default: System.out.println("Method doesn't exist");
                break;
        }
    }
}
