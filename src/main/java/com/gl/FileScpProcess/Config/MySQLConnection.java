/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.gl.FileScpProcess.Config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jasypt.util.text.BasicTextEncryptor;

/**
 *
 * @author maverick
 */
public class MySQLConnection {

    Logger logger = LogManager.getLogger(MySQLConnection.class);

    public static PropertyReader propertyReader;

    public Connection getConnection() {
        if (Objects.isNull(propertyReader)) {
            propertyReader = new PropertyReader();
        }
        Connection conn = null;
        try {

            String jdbcDriver = propertyReader.getConfigPropValue("jdbc_driver").trim();
            String dbURL = propertyReader.getConfigPropValue("db_url").trim();
            String username = propertyReader.getConfigPropValue("dbUsername").trim();
            String password = propertyReader.getConfigPropValue("dbPassword").trim();
            if (jdbcDriver.contains("mysql")) {
                password = propertyReader.getConfigPropValue("dbPassword").trim();
            } else {
                password = decryptor(propertyReader.getConfigPropValue("dbEncyptPassword").trim());
            }

            Class.forName(jdbcDriver);
            conn = DriverManager.getConnection(dbURL, username, password);

            if (conn != null) {
                System.out.println("Connected with connection #2");
            }

            logger.info("Connnection created successfully " + conn + " .. " + java.time.LocalDateTime.now());
            return conn;
        } catch (Exception e) {
            logger.error(" Error : : " + e + " :  " + e.getLocalizedMessage());
            try {
                conn.close();
            } catch (SQLException ex) {
                logger.error(" SQLException : " + ex + " :  " + java.time.LocalDateTime.now());
            }
            System.exit(0);
            return null;
        }
    }

    public String decryptor(String encryptedText) {
        BasicTextEncryptor encryptor = new BasicTextEncryptor();
        encryptor.setPassword(System.getenv("JASYPT_ENCRYPTOR_PASSWORD"));
        return encryptor.decrypt(encryptedText);
    }

}

//            final String passwordDecryptor = propertyReader.getPropValue("password_decryptor").trim().replace("${APP_HOME}", System.getenv("APP_HOME"));
//            logger.info("passwordDecryptor ." + passwordDecryptor);
//            final String PASS = getPassword(passwordDecryptor);
//   Class.forName("oracle.jdbc.driver.­OracleDriver");
// Class.forName("oracle.jdbc.OracleDriver");
//            String PASS = "CRESTELCEIR123#";
//            logger.info(JDBC_DRIVER + " :: " + DB_URL + " :: " + USER + " :: " + PASS);
//            logger.info("Connnection  Init " + java.time.LocalDateTime.now());
//
//            //Class.forName(JDBC_DRIVER);
//         //   Class.forName("oracle.jdbc.driver.­OracleDriver");
//            Class.forName("oracle.jdbc.OracleDriver");  //oracle.jdbc.OracleDriver
//
//            String dbURL1 = "jdbc:oracle:thin:CRESTELCEIR/CRESTELCEIR123#@64.227.137.112:1521/ORCLPDB1";
//
//            Connection conn1 = DriverManager.getConnection(dbURL1);
//            if (conn1 != null) {
//                System.out.println("Connected with connection #1");
//            }
//            String dbURL2 = "jdbc:oracle:thin:@64.227.137.112:1521/ORCLPDB1";
//            String username = "CRESTELCEIR";
//            String password = "CRESTELCEIR123#";
//            Connection conn2 = DriverManager.getConnection(dbURL2, username, password);
//
//            if (conn2 != null) {
//                System.out.println("Connected with connection #2");
//            }
//
//            conn = DriverManager.getConnection(DB_URL, USER, PASS);
//            conn.setAutoCommit(true);

//    String getPassword(String passwordDecryptor) {
//        String line = null;
//        String response = null;
//        try {
//            String cmd = "java -jar " + passwordDecryptor + "  spring.datasource.password";
//            logger.debug("cmd to  run::" + cmd);
//            Process pro = Runtime.getRuntime().exec(cmd);
//            BufferedReader in = new BufferedReader(
//                    new InputStreamReader(pro.getInputStream()));
//            while ((line = in.readLine()) != null) {
//                logger.debug("Response::" + line);
//                response = line;
//            }
//            return response;
//        } catch (Exception e) {
//            logger.info("Error  Password " + e);
//            e.printStackTrace();
//            return null;
//        }
//    }