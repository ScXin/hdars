package com.hlsii.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import java.io.File;
//import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author shxin
 * @date 2021/1/5
 */
@Component
public class ScheduledDeleteTask {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Scheduled注解，定时删除fixrate时间之前生成的下载文件
     */
    @Scheduled(fixedRate = 18000000)
    public void deleteRecordFile() {
        Date dateNow = new Date();
        long limitTime = dateNow.getTime() - 1800000;

        File file = new File(ConfigUtil.getConfigFilesDir() + "/records");
        File[] fileList = file.listFiles();
        for (int i = 0; i < fileList.length; i++) {
            if (fileList[i].lastModified() < limitTime) {
                if (fileList[i].delete()) {
                } else {
                    logger.error("Delete file failed!");
                }
            }
        }
    }

    //由于WebSocket 与 @Scheduled 注解不兼容，需要自己定义ThreadPoolTaskScheduler类的Bean
    @Bean
    public ThreadPoolTaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
        return taskScheduler;
    }
}


