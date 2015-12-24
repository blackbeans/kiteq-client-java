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


    /**
     * 测试模式时，可以自己指定日志目录，因为Mac上默认无法在/home/下建立文件夹。
     */
    public static void initLog(String logFileNameSuffix) {
        if (inited) {
            return;
        }

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(LogInitUtils.class.getClassLoader());
        try {
            // Load kiteq log configuration file.
            PropertyConfigurator.configure(log4jConfiguration);

            // Change log file name: append file name suffix.
            FileAppender fileAppender = null;
            for (Enumeration<?> appenders = Logger.getRootLogger().getAllAppenders(); (null == fileAppender)
                    && appenders.hasMoreElements();) {
                Appender appender = (Appender) appenders.nextElement();
                if (FileAppender.class.isInstance(appender)) {
                    FileAppender logFileAppender = (FileAppender) appender;
                    String oldLogFilePath = logFileAppender.getFile();
                    String newLogFilePath = StringUtils.substringBeforeLast(oldLogFilePath, ".log") + "-"
                            + logFileNameSuffix + ".log";
                    File logFile = new File(newLogFilePath);
                    logFileAppender.setFile(logFile.getAbsolutePath());
                    logFileAppender.activateOptions(); // Important!
                }
            }

        } finally {
            Thread.currentThread().setContextClassLoader(classLoader);
            inited =true;
        }
    }

}
