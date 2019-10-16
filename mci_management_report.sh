#!/usr/bin/env bash
cd /home/mohamad/Desktop/rpat_opt_scrips/Java_code/MciManagementReport

export rpatMciMailServer="mail.fwutech.com"
export rpatMciMailUsername="emailuser"
export rpatMciMailPassword="emailpass"
export rpatMciMailRecipients="m.example@example.com"
export rpatMciDatabaseHuawei="jdbc:postgresql://sample_ip:5433/rpat"
export rpatMciDatabaseUserHuawei="User"
export rpatMciDatabasePasswordHuawei="Password"

export rpatMciDatabaseNokia="jdbc:postgresql://sample_ip:5433/rpat"
export rpatMciDatabaseUserNokia="user"
export rpatMciDatabasePasswordNokia="password"


/usr/bin/java -jar /home/mohamad/Desktop/rpat_opt_scrips/Java_code/MciManagementReport/target/MciManagementReport-0.0.1-SNAPSHOT-jar-with-dependencies.jar
