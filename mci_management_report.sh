#!/usr/bin/env bash
cd /home/mohamad/Desktop/rpat_opt_scrips/Java_code/MciManagementReport

export rpatMciMailServer="mail.fwutech.com"
export rpatMciMailUsername="rpat-notification@fwutech.com"
export rpatMciMailPassword="lbzTxgr2"
export rpatMciMailRecipients="m.ghafari@fwutech.com, a.salari@fwutech.com, a.alaee@fwutech.com"
# export rpatMciMailRecipients="a.alaee@fwutech.com, a.salari@fwutech.com, a.alaee@fwutech.com"
export rpatMciDatabaseHuawei="jdbc:postgresql://10.186.86.5:5433/rpat"
export rpatMciDatabaseUserHuawei="postgres"
export rpatMciDatabasePasswordHuawei="123456"

export rpatMciDatabaseNokia="jdbc:postgresql://10.186.86.4:5433/rpat"
export rpatMciDatabaseUserNokia="postgres"
export rpatMciDatabasePasswordNokia="123456"


/usr/bin/java -jar /home/mohamad/Desktop/rpat_opt_scrips/Java_code/MciManagementReport/target/MciManagementReport-0.0.1-SNAPSHOT-jar-with-dependencies.jar
