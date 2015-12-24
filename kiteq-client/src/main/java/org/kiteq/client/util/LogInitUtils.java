package org.kiteq.client.util;


import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.File;
import java.util.Enumeration;
import java.util.Properties;


public class LogInitUtils {
    private static volatile boolean inited = false;

    private static final Properties log4jConfiguration = new Properties();

    static {


        log4jConfiguration.put("log4j.logger.org.kiteq", "INFO,kiteQFile");
        log4jConfiguration.put("log4j.additivity.org.kiteq", "false");
        // Log settings
        log4jConfiguration.put("log4j.appender.kiteQFile", "org.apache.log4j.RollingFileAppender");
        log4jConfiguration.put("log4j.appender.kiteQFile.MaxFileSize", "100MB");
        log4jConfiguration.put("log4j.appender.kiteQFile.MaxBackupIndex", "15");
        log4jConfiguration.put("log4j.appender.kiteQFile.Threshold", "INFO");
        log4jConfiguration.put("log4j.appender.kiteQFile.layout", "org.apache.log4j.PatternLayout");
        log4jConfiguration.put("log4j.appender.kiteQFile.layout.ConversionPattern", "%d %p [%c] \r\n\t%m%n");
    }


    public static void initLog(String logFileNameSuffix) {
        if (inited) {
            return;
        }

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(LogInitUtils.class.getClassLoader());
        try {


            // Change log file name: append file name suffix.
            FileAppender fileAppender = null;
            Enumeration<?> appenders = Logger.getRootLogger().getAllAppenders();
            for (; (null == fileAppender)
                    && appenders.hasMoreElements(); ) {
                Appender appender = (Appender) appenders.nextElement();
                if (FileAppender.class.isInstance(appender)) {
                    FileAppender logFileAppender = (FileAppender) appender;
                    String oldLogFilePath = logFileAppender.getFile();
                    String newLogFilePath = StringUtils.substringBeforeLast(oldLogFilePath, "/") + "/kiteq-"
                            + logFileNameSuffix + ".log";
                    log4jConfiguration.setProperty("log4j.appender.kiteQFile.file",newLogFilePath);
                    break;
                }
            }

            if (!inited) {
                // Load kiteq log configuration file.
                PropertyConfigurator.configure(log4jConfiguration);
            }

        } finally {
            Thread.currentThread().setContextClassLoader(classLoader);
            inited = true;
        }
    }

}
