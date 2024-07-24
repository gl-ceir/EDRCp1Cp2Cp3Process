package com.gl.FileScpProcess.P5Process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;

import static com.gl.FileScpProcess.P5Process.QueryExecuter.runQuery;

public class CustomImeiPairProcess {

    static Logger logger = LogManager.getLogger(CustomImeiPairProcess.class);
    public static void p5(Connection conn) {
        var table = "gdce_data";
        updateNwl(conn);
        startService(conn, table);
    }

    public static void updateNwl(Connection conn) {
        var q = "update app.national_whitelist set gdce_imei_status =1, gdce_modified_time =CURRENT_TIMESTAMP" +
                "  where gdce_imei_status in (0,3)   and imei in( select imei from app.gdce_data where created_on >= ( select IFNULL(value, '2000-01-01') from sys_param where tag ='gdce_register_imei_update_last_run_time' )  )  ";
        runQuery(conn, q);
    }

    public static void startService(Connection conn, String table) {
        // gdce_data
        var getString = " select a.imei  from " + table + " a, imei_pair_detail b " +
                " where a.imei=b.imei and a.created_on >= ( select IFNULL(value, '2000-01-01') from sys_param where tag ='gdce_register_imei_update_last_run_time' ) ";

        insertNwlFromGdceOnPairRecordTime(conn, getString);

        insertInImeiPairHis(conn, getString);

        insertInExceptionListHis(conn, getString);
        deleteFromEXceptionList(conn, getString);

        insertInBlackListHis(conn, getString);
        deleteFromBlackList(conn, getString);
        removeAutoFromBlackList(conn, getString);
        deleteFromImeiPair(conn, " select imei from " + table + " ");
        updateGdceDateTime(conn);

    }

    private static void insertNwlFromGdceOnPairRecordTime(Connection conn, String getString) {
        String a = "insert into  app.national_whitelist(action,actual_imei,actual_operator,created_filename,created_on_date,failed_rule_date, " +
                "failed_rule_id,failed_rule_name,feature_name,foreign_rule, imei,imei_arrival_time,imsi,is_used_device_imei,mobile_operator, " +
                "msisdn,period, raw_cdr_file_name,record_time,record_type,server_origin,source,system_type,tac,tax_paid, is_test_imei, " +
                "updated_filename,update_imei_arrival_time,update_raw_cdr_file_name,update_source ,gdce_imei_status,gdce_modified_time) " +
                "select action,actual_imei,actual_operator,create_filename,created_on,failed_rule_date, " +
                "failed_rule_id,failed_rule_name,feature_name,foregin_rule, imei,imei_arrival_time,imsi, is_used,mobile_operator, " +
                "msisdn,period,raw_cdr_file_name,record_time,record_type,server_origin,source,system_type,tac,tax_paid,test_imei, " +
                "update_filename,update_imei_arrival_time,update_raw_cdr_file_name,update_source , 1, CURRENT_TIMESTAMP from active_unique_imei  where imei in(select distinct imei from imei_pair_detail where imei in (SELECT  gdce_data.imei FROM gdce_data  LEFT JOIN national_whitelist on gdce_data.imei = national_whitelist.imei WHERE  national_whitelist.imei IS NULL and  imei_pair_detail.record_time is not null and gdce_data.created_on >= ( select IFNULL(value, '2000-01-01') from sys_param where tag ='gdce_register_imei_update_last_run_time' )  ))   ";
        runQuery(conn, a);
    }

    private static void updateGdceDateTime(Connection conn) {
        String a = "update sys_param set value =CURRENT_TIMESTAMP where tag ='gdce_register_imei_update_last_run_time' ";
        runQuery(conn, a);
    }


    private static void insertInExceptionListHis(Connection conn, String str) {
        String q = "insert into exception_list_his (actual_imei, imei,imsi , msisdn ,operator_id , operator_name, complaint_type, expiry_date , mode_type , request_type , txn_id , user_id , user_type ,tac ,remarks,source ,action ,action_remark ,operation )" +
                " select actual_imei, imei,imsi , msisdn ,operator_id , operator_name, complaint_type, expiry_date , mode_type , request_type , txn_id , user_id , user_type ,tac ,remarks,'GDCE_TAX_PAID' ,'DELETE','GDCE_TAX_PAID' , 0 from exception_list " +
                " where imei in( " + str + " )";
        runQuery(conn, q);
    }

    private static void deleteFromEXceptionList(Connection conn, String str) {
        String q = "delete from exception_list  where imei in (" + str + ")  ";
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

    private static void removeAutoFromBlackList(Connection conn, String str) {
        String q = "UPDATE black_list SET source = TRIM(BOTH ',' FROM REPLACE(CONCAT(',', source, ','), ',AUTO,', ','))WHERE source LIKE '%AUTO%'  and  imei in(" + str + ")  ";
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