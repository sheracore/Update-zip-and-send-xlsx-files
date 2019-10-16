/**
 * 
 */
package com.fwuetch.mciManagementReport;

import com.sun.mail.smtp.SMTPTransport;

import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.activation.DataHandler;
import javax.activation.FileDataSource;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
 
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFTable;
import org.apache.poi.ss.util.AreaReference;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.usermodel.XSSFRow;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.io.FileInputStream; 

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import java.util.ArrayList;
import java.util.Arrays;


/**
 * @author Amin Alaee
 *
 */
public class Main {

    static String mailServer = System.getenv("rpatMciMailServer");
    static String mailUsername = System.getenv("rpatMciMailUsername");
    static String mailPassword = System.getenv("rpatMciMailPassword");
    static String mailRecipients = System.getenv("rpatMciMailRecipients");

    static String databaseUrlHuawei = System.getenv("rpatMciDatabaseHuawei");
    static String databaseUserHuawei = System.getenv("rpatMciDatabaseUserHuawei");
    static String databasePasswordHuawei = System.getenv("rpatMciDatabasePasswordHuawei");

    static String databaseUrlNokia = System.getenv("rpatMciDatabaseNokia");
    static String databaseUserNokia = System.getenv("rpatMciDatabaseUserNokia");
    static String databasePasswordNokia = System.getenv("rpatMciDatabasePasswordNokia");

    static String regionQueryHuawei =  String.join(
        " ",
        "SELECT T1.date_time::date, T1.geo_value, T1.formula_cal, T2.formula_cal, T3.formula_cal, T4.formula_cal, T5.formula_cal, T6.formula_cal, T7.formula_cal, T8.formula_cal, T9.formula_cal, T10.formula_cal, T11.formula_cal, T12.formula_cal, T13.formula_cal, T14.formula_cal, T15.formula_cal, T16.formula_cal, T17.formula_cal, T18.formula_cal, T19.formula_cal, T20.formula_cal FROM",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_2g_region_dly WHERE formula_code = 61 AND date_time >= current_date - 30) AS T1",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_2g_region_dly WHERE formula_code = 87 AND date_time >= current_date - 30) AS T2",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_2g_region_dly WHERE formula_code = 205 AND date_time >= current_date - 30) AS T3",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_2g_region_dly WHERE formula_code = 93 AND date_time >= current_date - 30) AS T4",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_2g_region_dly WHERE formula_code = 89 AND date_time >= current_date - 30) AS T5",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_rnc_3g_region_dly WHERE formula_code = 602 AND date_time >= current_date - 30) AS T6",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_rnc_3g_region_dly WHERE formula_code = 197 AND date_time >= current_date - 30) AS T7",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_nodeb_3g_region_dly WHERE formula_code = 157 AND date_time >= current_date - 30) AS T8",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_rnc_3g_region_dly WHERE formula_code = 82 AND date_time >= current_date - 30) AS T9",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_rnc_3g_region_dly WHERE formula_code = 223 AND date_time >= current_date - 30) AS T10",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_rnc_3g_region_dly WHERE formula_code = 552 AND date_time >= current_date - 30) AS T11",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_rnc_3g_region_dly WHERE formula_code = 179 AND date_time >= current_date - 30) AS T12",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_nodeb_3g_region_dly WHERE formula_code = 99 AND date_time >= current_date - 30) AS T13",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_lte_region_dly WHERE formula_code = 452 AND date_time >= current_date - 30) AS T14",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_lte_region_dly WHERE formula_code = 423 AND date_time >= current_date - 30) AS T15",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_lte_region_dly WHERE formula_code = 437 AND date_time >= current_date - 30) AS T16",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_lte_region_dly WHERE formula_code = 426 AND date_time >= current_date - 30) AS T17",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_lte_region_dly WHERE formula_code = 603 AND date_time >= current_date - 30) AS T18",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_lte_region_dly WHERE formula_code = 424 AND date_time >= current_date - 30) AS T19",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_lte_region_dly WHERE formula_code = 598 AND date_time >= current_date - 30) AS T20",
        "USING (geo_value, date_time)",
        "WHERE T1.formula_cal IS NOT NULL"
    );

    static String provinceQueryHuawei =  String.join(
        " ",
        "SELECT T1.date_time::date, T1.geo_value, T1.formula_cal, T2.formula_cal, T3.formula_cal, T4.formula_cal, T5.formula_cal, T6.formula_cal, T7.formula_cal, T8.formula_cal, T9.formula_cal, T10.formula_cal, T11.formula_cal, T12.formula_cal, T13.formula_cal, T14.formula_cal, T15.formula_cal, T16.formula_cal, T17.formula_cal, T18.formula_cal, T19.formula_cal, T20.formula_cal FROM",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_2g_province_dly WHERE formula_code = 61 AND date_time >= current_date - 30) AS T1",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_2g_province_dly WHERE formula_code = 87 AND date_time >= current_date - 30) AS T2",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_2g_province_dly WHERE formula_code = 205 AND date_time >= current_date - 30) AS T3",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_2g_province_dly WHERE formula_code = 93 AND date_time >= current_date - 30) AS T4",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_2g_province_dly WHERE formula_code = 89 AND date_time >= current_date - 30) AS T5",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_rnc_3g_province_dly WHERE formula_code = 602 AND date_time >= current_date - 30) AS T6",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_rnc_3g_province_dly WHERE formula_code = 197 AND date_time >= current_date - 30) AS T7",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_nodeb_3g_province_dly WHERE formula_code = 157 AND date_time >= current_date - 30) AS T8",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_rnc_3g_province_dly WHERE formula_code = 82 AND date_time >= current_date - 30) AS T9",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_rnc_3g_province_dly WHERE formula_code = 223 AND date_time >= current_date - 30) AS T10",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_rnc_3g_province_dly WHERE formula_code = 552 AND date_time >= current_date - 30) AS T11",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_rnc_3g_province_dly WHERE formula_code = 179 AND date_time >= current_date - 30) AS T12",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_nodeb_3g_province_dly WHERE formula_code = 99 AND date_time >= current_date - 30) AS T13",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_lte_province_dly WHERE formula_code = 452 AND date_time >= current_date - 30) AS T14",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_lte_province_dly WHERE formula_code = 423 AND date_time >= current_date - 30) AS T15",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_lte_province_dly WHERE formula_code = 437 AND date_time >= current_date - 30) AS T16",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_lte_province_dly WHERE formula_code = 426 AND date_time >= current_date - 30) AS T17",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_lte_province_dly WHERE formula_code = 603 AND date_time >= current_date - 30) AS T18",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_lte_province_dly WHERE formula_code = 424 AND date_time >= current_date - 30) AS T19",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM huawei_lte_province_dly WHERE formula_code = 598 AND date_time >= current_date - 30) AS T20",
        "USING (geo_value, date_time)",
        "WHERE T1.formula_cal IS NOT NULL"
    );

    static String regionQueryNokia =  String.join(
        " ",
        "SELECT T1.date_time::date, T1.geo_value, T1.formula_cal, T2.formula_cal, T3.formula_cal, T4.formula_cal, T5.formula_cal FROM",
        "(SELECT geo_value, date_time, formula_cal FROM nokia_2g_region_dly WHERE formula_code = 407 AND date_time >= current_date - 30) AS T1",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM nokia_2g_region_dly WHERE formula_code = 537 AND date_time >= current_date - 30) AS T2",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM nokia_2g_region_dly WHERE formula_code = 415 AND date_time >= current_date - 30) AS T3",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM nokia_2g_region_dly WHERE formula_code = 409 AND date_time >= current_date - 30) AS T4",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM nokia_2g_region_dly WHERE formula_code = 417 AND date_time >= current_date - 30) AS T5",
        "USING (geo_value, date_time)",
        "WHERE T1.formula_cal IS NOT NULL"
    );

    static String provinceQueryNokia =  String.join(
        " ",
        "SELECT T1.date_time::date, T1.geo_value, T1.formula_cal, T2.formula_cal, T3.formula_cal, T4.formula_cal, T5.formula_cal FROM",
        "(SELECT geo_value, date_time, formula_cal FROM nokia_2g_province_dly WHERE formula_code = 407 AND date_time >= current_date - 30) AS T1",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM nokia_2g_province_dly WHERE formula_code = 537 AND date_time >= current_date - 30) AS T2",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM nokia_2g_province_dly WHERE formula_code = 415 AND date_time >= current_date - 30) AS T3",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM nokia_2g_province_dly WHERE formula_code = 409 AND date_time >= current_date - 30) AS T4",
        "USING (geo_value, date_time)",
        "INNER JOIN",
        "(SELECT geo_value, date_time, formula_cal FROM nokia_2g_province_dly WHERE formula_code = 417 AND date_time >= current_date - 30) AS T5",
        "USING (geo_value, date_time)",
        "WHERE T1.formula_cal IS NOT NULL"
    );

    static String dailyAvailabilityQueryHuawei =  String.join(
        " ",
        "(SELECT TO_CHAR(date_time::date, 'mm/dd/yyyy'),'2G' ,SUBSTRING(geo_value,0,3) ,parent ,geo_value ,formula_cal ,case when (formula_cal<=100 and formula_cal>99) then 'Normal' when (formula_cal<=99 and formula_cal>97) then 'Minor' when (formula_cal<=97 and formula_cal>90) then 'Major'  when (formula_cal<=90 and formula_cal>0.01) then 'Critical' when (formula_cal<=0.01 and formula_cal>=0) then 'Down' end from huawei_2g_cell_dly where  formula_code=93 and date_time>= (current_date-1 || ' ' || '00:00:00')::timestamp and date_time<= (current_date-1 || ' ' || '23:59:59')::timestamp)",
        "union",
        "(SELECT TO_CHAR(date_time::date, 'mm/dd/yyyy'),'3G' ,SUBSTRING(geo_value,0,3) ,parent ,geo_value ,formula_cal ,case when (formula_cal<=100 and formula_cal>99) then 'Normal' when (formula_cal<=99 and formula_cal>97) then 'Minor' when (formula_cal<=97 and formula_cal>90) then 'Major'  when (formula_cal<=90 and formula_cal>0.01) then 'Critical' when (formula_cal<=0.01 and formula_cal>=0) then 'Down' end from huawei_rnc_3g_cell_dly where  formula_code=552 and date_time>= (current_date-1 || ' ' || '00:00:00')::timestamp and date_time<= (current_date-1 || ' ' || '23:59:59')::timestamp)",
        "union",
        "(SELECT TO_CHAR(date_time::date, 'mm/dd/yyyy'),'4G' ,parent ,'-' ,geo_value ,formula_cal ,case when (formula_cal<=100 and formula_cal>99) then 'Normal' when (formula_cal<=99 and formula_cal>97) then 'Minor' when (formula_cal<=97 and formula_cal>90) then 'Major'  when (formula_cal<=90 and formula_cal>0.01) then 'Critical' when (formula_cal<=0.01 and formula_cal>=0) then 'Down' end from huawei_lte_cell_dly where  formula_code=603 and date_time>= (current_date-1 || ' ' || '00:00:00')::timestamp and date_time<= (current_date-1 || ' ' || '23:59:59')::timestamp)"
        );

    static String dailyAvailabilityQueryNokia = "SELECT TO_CHAR(date_time::date, 'mm/dd/yyyy'),'2G',SUBSTRING(geo_value,0,3) ,parent ,geo_value ,formula_cal ,case when (formula_cal<=100 and formula_cal>99) then 'Normal' when (formula_cal<=99 and formula_cal>97) then 'Minor' when (formula_cal<=97 and formula_cal>90) then 'Major'  when (formula_cal<=90 and formula_cal>0.01) then 'Critical' when (formula_cal<=0.01 and formula_cal>=0) then 'Down' end from nokia_2g_cell_dly where  formula_code=415 and date_time>= (current_date-1 || ' ' || '00:00:00')::timestamp and date_time<= (current_date-1 || ' ' || '23:59:59')::timestamp and parent in ('B5450N','B5762N','B5766N','B5775N','B5810N','B5871N')";
    
    static String consecutiveAvailabilityQueryHuawei =  String.join(
        " ",
        "(select '2G' ,SUBSTRING(geo_value,0,3) ,parent,geo_value,count(geo_value),formula_cal,'Down' from ",
        "(SELECT date_time::date,parent,geo_value ,formula_cal from huawei_2g_cell_dly where  formula_code=93 and date_time>= (current_date-7 || ' ' || '00:00:00')::timestamp and date_time<= (current_date-1 || ' ' || '23:59:59')::timestamp and formula_cal =0) as h",
        "group by geo_value,parent,formula_cal",
        "having count(geo_value)>1)",
        "union",
        "(select '3G' ,SUBSTRING(geo_value,0,3) ,parent,geo_value,count(geo_value),formula_cal,'Down' from ",
        "(SELECT date_time::date,parent,geo_value ,formula_cal from huawei_rnc_3g_cell_dly where  formula_code=552 and date_time>= (current_date-7 || ' ' || '00:00:00')::timestamp and date_time<= (current_date-1 || ' ' || '23:59:59')::timestamp and formula_cal =0) as h",
        "group by geo_value,parent,formula_cal",
        "having count(geo_value)>1)",
        "union",
        "(select '4G' ,parent ,'-',geo_value,count(geo_value),formula_cal,'Down' from ",
        "(SELECT date_time::date,parent,geo_value ,formula_cal from huawei_lte_cell_dly where  formula_code=603 and date_time>= (current_date-7 || ' ' || '00:00:00')::timestamp and date_time<= (current_date-1 || ' ' || '23:59:59')::timestamp and formula_cal =0) as h",
        "group by geo_value,parent,formula_cal",
        "having count(geo_value)>1)"
        );

    static String consecutiveAvailabilityQueryNokia =  String.join(
        " ",
        "select '2G' ,SUBSTRING(geo_value,0,3) ,parent,geo_value,count(geo_value),formula_cal,'Down' from nokia_2g_cell_dly where  formula_code=415 and date_time>= (current_date-7 || ' ' || '00:00:00')::timestamp and date_time<= (current_date-1 || ' ' || '23:59:59')::timestamp and formula_cal =0",
        "group by geo_value,parent,formula_cal",
        "having count(geo_value)>1"
        );

    static String weeklyAvalabilitywholeweekQuery =  String.join(
        " ",
        "select '2G' ,'Huawei' ,SUBSTRING(geo_value,0,3) ,parent ,geo_value ,",
        "case when (min(formula_cal)<=99 and min(formula_cal)>97) then 'Minor' when (min(formula_cal)<=97 and min(formula_cal)>90) then 'Major'  when (min(formula_cal)<=90 and min(formula_cal)>0.01) then 'Critical' end",
        ",count ('Cell') ",
        "from huawei_2g_cell_dly where formula_code=93 ",
        "and date_time between (select cast(date_trunc('week', current_date) as date) -2+interval '0 hour') and",
        "(select cast(date_trunc('week', current_date) as date) + 5+interval '0 hour') ",
        "and formula_cal<=99 and formula_cal>0.01 ",
        "group by geo_value,parent ",
        "union ",
        "select '3G' ,'Huawei' ,SUBSTRING(geo_value,0,3) ,parent ,geo_value ,",
        "case when (min(formula_cal)<=99 and min(formula_cal)>97) then 'Minor' when (min(formula_cal)<=97 and min(formula_cal)>90) then 'Major'  when (min(formula_cal)<=90 and min(formula_cal)>0.01) then 'Critical' end",
        ",count ('Cell') ",
        "from huawei_rnc_3g_cell_dly where formula_code=552 ",
        "and date_time between (select cast(date_trunc('week', current_date) as date) -2+interval '0 hour') and",
        "(select cast(date_trunc('week', current_date) as date) + 5+interval '0 hour') ",
        "and formula_cal<=99 and formula_cal>0.01 ",
        "group by geo_value,parent",
        "union",
        "select '4G' ,'Huawei' ,SUBSTRING(geo_value,0,3) ,parent ,geo_value ,",
        "case when (min(formula_cal)<=99 and min(formula_cal)>97) then 'Minor' when (min(formula_cal)<=97 and min(formula_cal)>90) then 'Major'  when (min(formula_cal)<=90 and min(formula_cal)>0.01) then 'Critical' end",
        ",count ('Cell') ",
        "from huawei_lte_cell_dly where formula_code=603 ",
        "and date_time between (select cast(date_trunc('week', current_date) as date) -2+interval '0 hour') and",
        "(select cast(date_trunc('week', current_date) as date) + 5+interval '0 hour') ",
        "and formula_cal<=99 and formula_cal>0.01 ",
        "group by geo_value,parent"
        );

    static String weeklyAvalabilityLastDayweekQuery =  String.join(
        " ",
        "select date_time,'2G' ,'Huawei',SUBSTRING(geo_value,0,3) ,parent ,geo_value ,formula_cal,",
        "case when formula_cal<=99 and formula_cal>97 then 'Minor' when formula_cal<=97 and formula_cal>90 then 'Major'  when formula_cal<=90 and formula_cal>0.01 then 'Critical' when formula_cal<=0.01 and formula_cal>=0 then 'Down'end",
        "from huawei_2g_cell_dly where formula_code=93 ",
        "and date_time=",
        "(select cast(date_trunc('week', current_date) as date) + 5+interval '0 hour' - interval '1 day') ",
        "and formula_cal<=99",
        "union",
        "select date_time,'3G' ,'Huawei',SUBSTRING(geo_value,0,3) ,parent ,geo_value ,formula_cal,",
        "case when formula_cal<=99 and formula_cal>97 then 'Minor' when formula_cal<=97 and formula_cal>90 then 'Major'  when formula_cal<=90 and formula_cal>0.01 then 'Critical' when formula_cal<=0.01 and formula_cal>=0 then 'Down'end",
        "from huawei_rnc_3g_cell_dly where formula_code=552 ",
        "and date_time=",
        "(select cast(date_trunc('week', current_date) as date) + 5+interval '0 hour' - interval '1 day') ",
        "and formula_cal<=99",
        "union",
        "select date_time,'4G' ,'Huawei',SUBSTRING(geo_value,0,3) ,parent ,geo_value ,formula_cal,",
        "case when formula_cal<=99 and formula_cal>97 then 'Minor' when formula_cal<=97 and formula_cal>90 then 'Major'  when formula_cal<=90 and formula_cal>0.01 then 'Critical' when formula_cal<=0.01 and formula_cal>=0 then 'Down'end",
        "from huawei_lte_cell_dly where formula_code=603 ",
        "and date_time=",
        "(select cast(date_trunc('week', current_date) as date) + 5+interval '0 hour' - interval '1 day') ",
        "and formula_cal<=99"
        );

    /**
     * @param args
     */
    public static void main(String[] args) {
        String date = new SimpleDateFormat("yyyyMMdd").format(new Date());
        List<String> files = new ArrayList<String>();

        Multipart mp = new MimeMultipart();

        doManagementReport(files);
        
        String inputFileAvailabilityDaily = "FARAFAN_Availability_Daily_Report.xlsx";
        String outputFileAvailabilityDaily = String.format("FARAFAN_Availability_Daily_Report%s.xlsx", date);
        String inputFileAvailabilityConsecutive = "FARAFAN_Availability_Consecutive_Down_Cell.xlsx";
        String outputFileAvailabilityConsecutive = String.format("FARAFAN_Availability_Consecutive_Down_Cell%s.xlsx", date);
        String inputFileAvailabilityweekly = "FARAFANـAvailabilityـWeeklyـReport.xlsx";
        String outputFileAvailabilityweekly =String.format("FARAFANـAvailabilityـWeeklyـReport(W26).xlsx", date);
        
        doAvailabilityReport(files, mp, inputFileAvailabilityDaily, outputFileAvailabilityDaily,date);
        doAvailabilityReport(files, mp, inputFileAvailabilityConsecutive, outputFileAvailabilityConsecutive,date);
        doAvailabilityReport(files, mp, inputFileAvailabilityweekly, outputFileAvailabilityweekly,date);

    }

    public static void doManagementReport(List<String> files) {
        String date = new SimpleDateFormat("yyyy_MM_dd").format(new Date());
        String inputFile = "Daily_Management_Level_Report.xlsx";
        String outputFile = String.format("Daily_Management_Level_Report_%s.xlsx", date);
         
        try {
            XSSFWorkbook workbook = new XSSFWorkbook(new File(inputFile));

            Connection connHuawei = DriverManager.getConnection(databaseUrlHuawei, databaseUserHuawei, databasePasswordHuawei);
            Connection connNokia = DriverManager.getConnection(databaseUrlNokia, databaseUserNokia, databasePasswordNokia);
            
            getDataHuawei(workbook, connHuawei, 0, regionQueryHuawei);
            getDataHuawei(workbook, connHuawei, 2, provinceQueryHuawei);

            getDataNokia(workbook, connNokia, 1, regionQueryNokia);
            getDataNokia(workbook, connNokia, 3, provinceQueryNokia);

            workbook.getCreationHelper().createFormulaEvaluator().evaluateAll();

            FileOutputStream outputStream = new FileOutputStream(outputFile);
            workbook.write(outputStream);
            workbook.close();
            outputStream.close();
            
            files.add(outputFile);
            // File file = new File(outputFile);
            // file.delete();

        } catch (IOException | EncryptedDocumentException | SQLException | InvalidFormatException ignore) {
            Date datetime = new Date();
            System.out.println(datetime.toString() + " Connection Failed");
            sleep();
            doManagementReport(files);
        }
    }

    public static void doAvailabilityReport(List<String> files, Multipart mp, String inputFile, String outputFile, String date){

        String dailyAvailabilityQueryHuawei_name = "dailyAvailabilityQueryHuawei";
        String dailyAvailabilityQueryNokia_name = "dailyAvailabilityQueryNokia";
        String consecutiveAvailabilityQueryHuawei_name = "consecutiveAvailabilityQueryHuawei";
        String consecutiveAvailabilityQueryNokia_name = "consecutiveAvailabilityQueryNokia";
        String weeklyAvalabilitywholeweekQuery_name = "weeklyAvalabilitywholeweekQuery";
        String weeklyAvalabilityLastDayweekQuery_name = "weeklyAvalabilityLastDayweekQuery";

        
        try{
        Connection connHuawei = DriverManager.getConnection(databaseUrlHuawei, databaseUserHuawei, databasePasswordHuawei);
        Connection connNokia = DriverManager.getConnection(databaseUrlNokia, databaseUserNokia, databasePasswordNokia);
       
        if(inputFile=="FARAFAN_Availability_Daily_Report.xlsx") {

            files.add(outputFile);
            doAvailabilityReportHelper(files, mp, inputFile, outputFile, connHuawei, 1, dailyAvailabilityQueryHuawei, dailyAvailabilityQueryHuawei_name, date);
            doAvailabilityReportHelper(files, mp, inputFile, outputFile, connNokia, 0, dailyAvailabilityQueryNokia, dailyAvailabilityQueryNokia_name, date);
            
            
            // zipFile(inputFile);
            // sendEmail(date, outputFile);
            // File file = new File(outputFile);
            // file.delete();
        }
        else if(inputFile=="FARAFAN_Availability_Consecutive_Down_Cell.xlsx") {

            files.add(outputFile);
            doAvailabilityReportHelper(files, mp, inputFile, outputFile, connHuawei, 1, consecutiveAvailabilityQueryHuawei, consecutiveAvailabilityQueryHuawei_name, date);
            doAvailabilityReportHelper(files, mp, inputFile, outputFile, connNokia, 0, consecutiveAvailabilityQueryNokia, consecutiveAvailabilityQueryNokia_name, date);
            
            // zipFile(inputFile);
            // sendEmail(date, outputFile);
            // File file = new File(outputFile);
            // file.delete();
        }
        else if(inputFile=="FARAFANـAvailabilityـWeeklyـReport.xlsx"){

            files.add(outputFile);
            doAvailabilityReportHelper(files, mp, inputFile, outputFile, connHuawei, 0, weeklyAvalabilitywholeweekQuery, weeklyAvalabilitywholeweekQuery_name, date);
            doAvailabilityReportHelper(files, mp, inputFile, outputFile, connHuawei, 1, weeklyAvalabilityLastDayweekQuery, weeklyAvalabilityLastDayweekQuery_name, date);
            

            // zipFile(inputFile);
            zipFiles(files);
            
            files.add("zipFiles.zip");
            String zipFile = "zipFiles.zip";
            for (int i = 0; i < files.size(); i++) {
                System.out.println(files.get(i));
            }
            sendEmail(mp, date, zipFile);
            for(int i=0; i< files.size(); i++){
                File file = new File(files.get(i));
                file.delete();
        }
        }
        }
        catch(EncryptedDocumentException | SQLException ex) { System.out.println(ex);}

    }

    public static void zipFiles(List<String> files){
         
        FileOutputStream fos = null;
        ZipOutputStream zipOut = null;
        FileInputStream fis = null;
        try {
            fos = new FileOutputStream("zipFiles.zip");
            zipOut = new ZipOutputStream(new BufferedOutputStream(fos));
            for(String filePath:files){
                File input = new File(filePath);
                fis = new FileInputStream(input);
                ZipEntry ze = new ZipEntry(input.getName());
                System.out.println("Zipping the file: "+input.getName());
                zipOut.putNextEntry(ze);
                byte[] tmp = new byte[4*1024];
                int size = 0;
                while((size = fis.read(tmp)) != -1){
                    zipOut.write(tmp, 0, size);
                }
                zipOut.flush();
                fis.close();
            }
            zipOut.close();
            System.out.println("Done... Zipped the files...");
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally{
            try{
                if(fos != null) fos.close();
            } catch(Exception ex){
                 
            }
        }
    }


    public static void doAvailabilityReportHelper(List<String> files, Multipart mp, String inputFile , String outputFile, Connection conn, Integer sheetNumber, String query, String query_name,String date){
        try {
            XSSFWorkbook workbook = new XSSFWorkbook(new File(inputFile));
            
            
            getDataAvailability(workbook, conn, sheetNumber, query, query_name);
            // getDataAvailability(workbook, connNokia, 0, dailyAvailabilityQueryNokia, dailyAvailabilityQueryNokia_name);
            // getDataAvailability(workbook, connHuawei, 1, consecutiveAvailabilityQueryHuawei, consecutiveAvailabilityQueryHuawei_name);
            // getDataAvailability(workbook, connNokia, 0, consecutiveAvailabilityQueryNokia, consecutiveAvailabilityQueryNokia_name);
            // getDataAvailability(workbook, connHuawei, 0, weeklyAvalabilitywholeweekQuery, weeklyAvalabilitywholeweekQuery_name);
            // getDataAvailability(workbook, connNokia, 1, weeklyAvalabilityLastDayweekQuery, weeklyAvalabilityLastDayweekQuery_name);
            


            workbook.getCreationHelper().createFormulaEvaluator().evaluateAll();

            FileOutputStream outputStream = new FileOutputStream(outputFile);
            workbook.write(outputStream);
            workbook.close();
            outputStream.close();

            }
        catch (IOException | EncryptedDocumentException | InvalidFormatException ignore) {
                Date datetime = new Date();
                System.out.println(datetime.toString() + " Connection Failed");
                sleep();
                doAvailabilityReport(files, mp, inputFile,outputFile,date);
        }

    }

    public static void getDataHuawei(XSSFWorkbook workbook, Connection conn, Integer sheetNumber, String query) {

        Integer rowsLength = 0;
        Integer rowNumber = 0;

        XSSFSheet sheet = workbook.getSheetAt(sheetNumber);
        XSSFTable table = sheet.getTables().get(0);

        try {
            Statement statement = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet data = statement.executeQuery(query);

            data.last();
            rowsLength = data.getRow() + 1;
            data.beforeFirst();

            while (data.next()) {
                rowNumber++;
                XSSFRow row = sheet.createRow(rowNumber);
                row.createCell(0).setCellValue((String) data.getString(1));
                row.createCell(1).setCellValue((String) data.getString(2));

                // Nokia 2G
                if (sheetNumber == 0) {
                    row.createCell(2).setCellFormula(String.format("SUMIFS(Table242[2G_CSSR_Nokia],Table242[date],A2:A%s,Table242[Region],B2:B%s)", rowsLength, rowsLength));
                    row.createCell(3).setCellFormula(String.format("SUMIFS(Table242[2G_CDR_Nokia],Table242[date],A2:A%s,Table242[Region],B2:B%s)", rowsLength, rowsLength));
                    row.createCell(4).setCellFormula(String.format("SUMIFS(Table242[2G_TCH_Availability_Nokia],Table242[date],A2:A%s,Table242[Region],B2:B%s)", rowsLength, rowsLength));
                    row.createCell(5).setCellFormula(String.format("SUMIFS(Table242[2G_OHSR_Nokia],Table242[date],A2:A%s,Table242[Region],B2:B%s)", rowsLength, rowsLength));
                    row.createCell(6).setCellFormula(String.format("SUMIFS(Table242[2G_tch_traffic_Nokia],Table242[date],A2:A%s,Table242[Region],B2:B%s)", rowsLength, rowsLength));
                }  else if (sheetNumber == 2) {
                    row.createCell(2).setCellFormula(String.format("SUMIFS(Table25[2G_CSSR_Nokia],Table25[PERIOD_START_TIME],A2:A%s,Table25[PROVINCE],B2:B%s)", rowsLength, rowsLength));
                    row.createCell(3).setCellFormula(String.format("SUMIFS(Table25[2G_CDR_Nokia],Table25[PERIOD_START_TIME],A2:A%s,Table25[PROVINCE],B2:B%s)", rowsLength, rowsLength));
                    row.createCell(4).setCellFormula(String.format("SUMIFS(Table25[2G_TCH_Availability_Nokia],Table25[PERIOD_START_TIME],A2:A%s,Table25[PROVINCE],B2:B%s)", rowsLength, rowsLength));
                    row.createCell(5).setCellFormula(String.format("SUMIFS(Table25[2G_OHSR_Nokia],Table25[PERIOD_START_TIME],A2:A%s,Table25[PROVINCE],B2:B%s)", rowsLength, rowsLength));
                    row.createCell(6).setCellFormula(String.format("SUMIFS(Table25[2G_tch_traffic_Nokia],Table25[PERIOD_START_TIME],A2:A%s,Table25[PROVINCE],B2:B%s)", rowsLength, rowsLength));
                }

                // Huawei 2G
                row.createCell(7).setCellValue((Double) data.getDouble(3) / 1024);
                row.createCell(8).setCellValue((Double) data.getDouble(4));
                row.createCell(9).setCellValue((Double) data.getDouble(5));
                row.createCell(10).setCellValue((Double) data.getDouble(6));
                row.createCell(11).setCellValue((Double) data.getDouble(7));


                // Huawei 3G
                row.createCell(12).setCellValue((Double) data.getDouble(8) / 1024);
                row.createCell(13).setCellValue((Double) data.getDouble(9) / 1024);
                row.createCell(14).setCellValue((Double) data.getDouble(10) / 1024);
                row.createCell(15).setCellValue((Double) data.getDouble(11));
                row.createCell(16).setCellValue((Double) data.getDouble(12));
                row.createCell(17).setCellValue((Double) data.getDouble(13));
                row.createCell(18).setCellValue((Double) data.getDouble(14));
                row.createCell(19).setCellValue((Double) data.getDouble(15));

                // Huawei LTE
                row.createCell(20).setCellValue((Double) data.getDouble(16) / 1024);
                row.createCell(21).setCellValue((Double) data.getDouble(17));
                row.createCell(22).setCellValue((Double) data.getDouble(18));
                row.createCell(23).setCellValue((Double) data.getDouble(19));
                row.createCell(24).setCellValue((Double) data.getDouble(20));
                row.createCell(25).setCellValue((Double) data.getDouble(21));
                row.createCell(26).setCellValue((Double) data.getDouble(22));

                if (sheetNumber == 0) {
                    writeRegionTargetsHuawei(row, rowsLength);
                } else if (sheetNumber == 2) {
                    writeProvinceTargetsHuawei(row, rowsLength);
                }

            }

        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        AreaReference reference = workbook.getCreationHelper().createAreaReference(
                    new CellReference("A1"), new CellReference(String.format("AQ%s", rowsLength)));
        table.setCellReferences(reference);

    }

    public static void getDataAvailability(XSSFWorkbook workbook, Connection conn, Integer sheetNumber, String query, String query_name) {
      
        Integer rowsLength = 0;
        Integer rowNumber = 0;
        
        XSSFSheet sheet = workbook.getSheetAt(sheetNumber);
        XSSFTable table = sheet.getTables().get(0);

        Map<String, String> dictionary = new HashMap<String, String>();

        dictionary.put("Hamadan", "HN");
        dictionary.put("Zanjan", "ZN");
        dictionary.put("Kermanshah", "KS");
        dictionary.put("Bushehr", "BU");
        dictionary.put("Hormozgan", "HZ");
        dictionary.put("Lorestan", "LN");
        dictionary.put("Kurdistan", "KD");
        dictionary.put("Ardabil", "AR");

        try {
            Statement statement = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet data = statement.executeQuery(query);

            data.last();
            rowsLength = data.getRow() + 1;
            data.beforeFirst();

            while (data.next()) {
                rowNumber++;

                XSSFRow row = sheet.createRow(rowNumber);
                

                // Huawei and Nokia
                row.createCell(0).setCellValue((String) data.getString(1));
                if (query_name=="consecutiveAvailabilityQueryHuawei"){
                    
                    if(data.getString(2).length() >2){
                        row.createCell(1).setCellValue((String) dictionary.get(data.getString(2)));

                        }
                    else{
                        row.createCell(1).setCellValue((String) data.getString(2));
                    }
                    }
                else{
                    row.createCell(1).setCellValue((String) data.getString(2));
                }
         
                if (query_name=="dailyAvailabilityQueryHuawei"){
                    
                    if(data.getString(3).length() >2){
                        row.createCell(2).setCellValue((String) dictionary.get(data.getString(3)));

                        }
                    else{
                        row.createCell(2).setCellValue((String) data.getString(3));
                    }
                    }
                else{
                    row.createCell(2).setCellValue((String) data.getString(3));
                }
                
                row.createCell(3).setCellValue((String) data.getString(4));
                if (query_name=="consecutiveAvailabilityQueryHuawei" || query_name=="consecutiveAvailabilityQueryNokia"){
                    row.createCell(4).setCellValue((Double) data.getDouble(5));
                }else{
                    row.createCell(4).setCellValue((String) data.getString(5));
                }
                
                if (query_name=="weeklyAvalabilitywholeweekQuery"){
                row.createCell(5).setCellValue((String) data.getString(6));
                }
                else{
                    row.createCell(5).setCellValue((Double) data.getDouble(6));
                }
                
                if (query_name=="weeklyAvalabilitywholeweekQuery" || query_name=="dailyAvailabilityQueryHuawei" || query_name=="dailyAvailabilityQueryNokia" || query_name=="consecutiveAvailabilityQueryHuawei" || query_name=="consecutiveAvailabilityQueryNokia" ){
                row.createCell(6).setCellValue((String) data.getString(7));
                }
                else{
                    row.createCell(6).setCellValue((Double) data.getDouble(7));
                } 

            }


        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        AreaReference reference = workbook.getCreationHelper().createAreaReference(
                    new CellReference("A1"), new CellReference(String.format("G%s", rowsLength)));
        table.setCellReferences(reference);
    }

    public static void getDataNokia(XSSFWorkbook workbook, Connection conn, Integer sheetNumber, String query) {

        Integer rowsLength = 0;
        Integer rowNumber = 0;

        XSSFSheet sheet = workbook.getSheetAt(sheetNumber);
        XSSFTable table = sheet.getTables().get(0);

        try {
            Statement statement = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet data = statement.executeQuery(query);

            data.last();
            rowsLength = data.getRow() + 1;
            data.beforeFirst();

            while (data.next()) {
                rowNumber++;
                XSSFRow row = sheet.createRow(rowNumber);
                row.createCell(0).setCellValue((String) data.getString(1));
                row.createCell(1).setCellValue((String) data.getString(2));

                // Nokia 2G
                row.createCell(2).setCellValue((Double) data.getDouble(3));
                row.createCell(3).setCellValue((Double) data.getDouble(4));
                row.createCell(4).setCellValue((Double) data.getDouble(5));
                row.createCell(5).setCellValue((Double) data.getDouble(6));
                row.createCell(6).setCellValue((Double) data.getDouble(7));
            }

        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        AreaReference reference = workbook.getCreationHelper().createAreaReference(
                    new CellReference("A1"), new CellReference(String.format("G%s", rowsLength)));
        table.setCellReferences(reference);

    }

    public static void sendEmail(Multipart mp, String date, String fileName) {
        Properties prop = System.getProperties();
        prop.put("mail.smtp.host", mailServer);
        prop.put("mail.smtp.auth", "true");
        prop.put("mail.smtp.port", "25");

        Session session = Session.getInstance(prop, null);
        Message msg = new MimeMessage(session);
        
        
        try {
            
            System.out.println(fileName);          
            msg.setFrom(new InternetAddress("rpat-notification@fwutech.com"));
            msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(mailRecipients, false));
            msg.setSubject("Automatic RPAT Daily Management Report " + date);

            MimeBodyPart body = new MimeBodyPart();
            FileDataSource fds = new FileDataSource(fileName);
            body.setDataHandler(new DataHandler(fds));
            body.setFileName(fds.getName());

            mp.addBodyPart(body);
            msg.setContent(mp);
            
   
            SMTPTransport transport = (SMTPTransport) session.getTransport("smtp");
            transport.connect(mailServer, mailUsername, mailPassword);
            transport.sendMessage(msg, msg.getAllRecipients());
            
            System.out.println("Response: " + transport.getLastServerResponse());

            transport.close();
        
        } catch (MessagingException e) {
            e.printStackTrace();
        }
    }

    public static void writeRegionTargetsHuawei(XSSFRow row, Integer rowsLength) {
        String baseFormula = String.format("VLOOKUP(B2:B%s", rowsLength);
        row.createCell(27).setCellFormula(baseFormula + ",Reg_Target!A:Q,2,0)");
        row.createCell(28).setCellFormula(baseFormula + ",Reg_Target!A:Q,3,0)");
        row.createCell(29).setCellFormula(baseFormula + ",Reg_Target!A:Q,4,0)");
        row.createCell(30).setCellFormula(baseFormula + ",Reg_Target!A:Q,5,0)");
        row.createCell(31).setCellFormula(baseFormula + ",Reg_Target!A:Q,6,0)");
        row.createCell(32).setCellFormula(baseFormula + ",Reg_Target!A:Q,7,0)");
        row.createCell(33).setCellFormula(baseFormula + ",Reg_Target!A:Q,8,0)");
        row.createCell(34).setCellFormula(baseFormula + ",Reg_Target!A:Q,9,0)");
        row.createCell(35).setCellFormula(baseFormula + ",Reg_Target!A:Q,10,0)");
        row.createCell(36).setCellFormula(baseFormula + ",Reg_Target!A:Q,11,0)");
        row.createCell(37).setCellFormula(baseFormula + ",Reg_Target!A:Q,12,0)");
        row.createCell(38).setCellFormula(baseFormula + ",Reg_Target!A:Q,13,0)");
        row.createCell(39).setCellFormula(baseFormula + ",Reg_Target!A:Q,15,0)");
        row.createCell(40).setCellFormula(baseFormula + ",Reg_Target!A:Q,14,0)");
        row.createCell(41).setCellFormula(baseFormula + ",Reg_Target!A:Q,16,0)");
        row.createCell(42).setCellFormula(baseFormula + ",Reg_Target!A:Q,17,0)");
    }

    public static void writeProvinceTargetsHuawei(XSSFRow row, Integer rowsLength) {
        String baseFormula = String.format("VLOOKUP(B2:B%s", rowsLength);
        row.createCell(27).setCellFormula(baseFormula + ",Pro_Target!A:Q,2,0)");
        row.createCell(28).setCellFormula(baseFormula + ",Pro_Target!A:Q,3,0)");
        row.createCell(29).setCellFormula(baseFormula + ",Pro_Target!A:Q,4,0)");
        row.createCell(30).setCellFormula(baseFormula + ",Pro_Target!A:Q,5,0)");
        row.createCell(31).setCellFormula(baseFormula + ",Pro_Target!A:Q,6,0)");
        row.createCell(32).setCellFormula(baseFormula + ",Pro_Target!A:Q,7,0)");
        row.createCell(33).setCellFormula(baseFormula + ",Pro_Target!A:Q,8,0)");
        row.createCell(34).setCellFormula(baseFormula + ",Pro_Target!A:Q,9,0)");
        row.createCell(35).setCellFormula(baseFormula + ",Pro_Target!A:Q,10,0)");
        row.createCell(36).setCellFormula(baseFormula + ",Pro_Target!A:Q,11,0)");
        row.createCell(37).setCellFormula(baseFormula + ",Pro_Target!A:Q,12,0)");
        row.createCell(38).setCellFormula(baseFormula + ",Pro_Target!A:Q,13,0)");
        row.createCell(39).setCellFormula(baseFormula + ",Pro_Target!A:Q,15,0)");
        row.createCell(40).setCellFormula(baseFormula + ",Pro_Target!A:Q,14,0)");
        row.createCell(41).setCellFormula(baseFormula + ",Pro_Target!A:Q,16,0)");
        row.createCell(42).setCellFormula(baseFormula + ",Pro_Target!A:Q,17,0)");
    }

    public static void sleep() {
        try {
            TimeUnit.MINUTES.sleep(10);    
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
        
    }
}
