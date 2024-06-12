package com.gl.FileScpProcess.P5Process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;

import static com.gl.FileScpProcess.P5Process.NwlCustomFlagProcess.runQuery;

public class CustomImeiPairProcess {

    static Logger log = LogManager.getLogger(CustomImeiPairProcess.class);

    public static void p5(Connection conn) {
        var getString = "gdce_data";
        startService(conn, getString);
    }

    public static void startService(Connection conn, String value) {

        var getString = " select a.imei  from "+ value +" a, imei_pair_detail b where a.imei=b.imei ";
        insertInExceptionListHis(conn, getString);
        deleteFromEXceptionList(conn, getString);

        insertInBlackListHis(conn, getString);
        deleteFromBlackList(conn, getString);

        insertInImeiPairHis(conn, getString);
        deleteFromImeiPair(conn, " select imei from "+ value + " ");
    }


    private static void insertInExceptionListHis(Connection conn, String str) {
        String q = "insert into exception_list_his (actual_imei, imei,imsi , msisdn ,operator_id , operator_name, complaint_type, expiry_date , mode_type , request_type , txn_id , user_id , user_type ,tac ,remarks,source ,action ,action_remark )" +
                " select actual_imei, imei,imsi , msisdn ,operator_id , operator_name, complaint_type, expiry_date , mode_type , request_type , txn_id , user_id , user_type ,tac ,remarks,source ,'DELETE','GDCE_TAX_PAID' from exception_list " +
                " where imei in( " + str + " )";
        runQuery(conn, q);
    }

    private static void deleteFromEXceptionList(Connection conn, String str) {
        String q = "delete from   exception_list  where imei in (" + str + ")  ";
        runQuery(conn, q);
    }

    private static void insertInImeiPairHis(Connection conn, String str) {
        String q = "insert into imei_pair_detail_his ( allowed_days,imei ,imsi,msisdn,pairing_date ,record_time,file_name,gsma_status,pair_mode, operator,expiry_date , action,action_remark) " +
                " select allowed_days,imei ,imsi,msisdn, pairing_date ,record_time, file_name,gsma_status, pair_mode, operator,expiry_date ,'DELETE','GDCE_TAX_PAID' from  imei_pair_detail " +
                "where imei in( " + str + " )";
        runQuery(conn, q);
    }

    private static void deleteFromImeiPair(Connection conn, String str) {
        String q = "delete from  imei_pair_detail  where imei in (" + str + ")  ";
        runQuery(conn, q);
    }

    private static void insertInBlackListHis(Connection conn, String str) {
        String q = "insert into black_list_his (actual_imei, imei,imsi , msisdn ,operator_id , operator_name, complaint_type, expiry_date , mode_type , request_type , txn_id , user_id , user_type ,tac ,remarks,source ,action ,action_remark ) " +
                " select actual_imei, imei,imsi , msisdn ,operator_id , operator_name, complaint_type, expiry_date , mode_type , request_type , txn_id , user_id , user_type ,tac ,remarks,source ,'DELETE','GDCE_TAX_PAID' from black_list" +
                " where imei in(" + str + ") and source='AUTO' ";
        runQuery(conn, q);
    }

    private static void deleteFromBlackList(Connection conn, String str) {
        String q = "delete from  black_list  where imei in (" + str + ") and source='AUTO' ";
        runQuery(conn, q);
    }


}

//    3)	Process will select all the pair for IMEI that is present in app.imei_pair_detail and IMEI found in custom DB and follow below steps for each PAIR:
//a.	Insert the IMEI and IMSI pair in app.imei_pair_detail_his with action DELETE and action_remark as “GDCE_TAX_PAID”
//b.	Delete the IMEI and IMSI pair from app.imei_pair_detail

//c.	Insert the record in app.exception_list_his with operation DELETE (0)  and source as “GDCE_TAX_PAID”
//d.	Delete the IMEI and IMSI pair from app.exception_list
//e.	Continue above steps for all pairs, once all pair done then follow below steps:

//i.	Select the IMEI is present in app.black_list because of pair limit over <source=”AUTO”>, in case found then follow below steps:
//        1.	Insert the record in app.black_list_his with operation DELETE(0) and source as “GDCE_TAX_PAID”
//        2.	Delete the IMEI from app.black_list
//        var q = "update app.national_whitelist set tax_paid =1 where tax_paid in (0,3)  " +
//                " and imei in(select imei from app.gdce_data)";