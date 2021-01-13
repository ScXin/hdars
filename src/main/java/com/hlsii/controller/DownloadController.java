package com.hlsii.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hazelcast.util.StringUtil;
import com.hlsii.commdef.*;
import com.hlsii.service.BufferedRetrieveService;
import com.hlsii.service.IDownloadService;
import com.hlsii.service.IRetrieveService;
import com.hlsii.util.ConfigUtil;
import com.hlsii.util.ExportUtil;
import com.hlsii.util.SiteConfigUtil;
import com.hlsii.vo.RetrieveData;
import com.hlsii.vo.ReturnWrap;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.impl.client.FutureRequestExecutionMetrics;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.embedded.undertow.ConfigurableUndertowWebServerFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import sun.security.krb5.Config;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author ScXin
 * @date 4/26/2020 6:27 PM
 */

@RequestMapping("/hdars/download")
@RestController
public class DownloadController {

    private static Logger logger = Logger.getLogger(DownloadController.class);


    @Autowired
    private IDownloadService downloadService;

    @Autowired
    private IRetrieveService retrieveService;


    private RetrieveParms retrieveParms;
    private long totalSize = 0;
    private ArrayList<String> pvNameList = new ArrayList<>();
    private Timestamp timestampPosition;


    @ApiOperation("最新重写的数据下载接口，每个PV分开下载，写入文件，本接口返回的是下载之后的文件名称")
    @GetMapping("/startDownload/{taskid}")
    public String startDownload(@PathVariable("taskid") String taskid) throws IOException {
        logger.info("start download task:" + taskid);
        DownloadTask task = downloadService.getTask(taskid);
        if (task == null || task.getState() != DownloadState.Created) {
            logger.info("Download task " + taskid + " not existed!");
            return null;
        }

        retrieveParms = task.getParms();
        if (retrieveService == null || retrieveParms == null) {
            logger.error(MessageFormat.format("The retrieveService==null?{0} or RetrieveParms==null?{1} passed in is null!",
                    retrieveService == null, retrieveParms == null));
            return null;
        }
        if (retrieveParms.getFrom() == null || retrieveParms.getTo() == null) {
            logger.error(MessageFormat.format("The retrieveParms.getFrom()==null?{0} or " +
                            "retrieveParms.getTo()==null?{1} passed in is null!",
                    retrieveParms.getFrom() == null, retrieveParms.getTo() == null));
            return null;
        }

        task.setState(DownloadState.Downloading);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
        Date date = new Date();
        String filename = simpleDateFormat.format(date) + ".txt";
        String fileAddr = ConfigUtil.getConfigFilesDir() + "/records/" + filename;
        File file = new File(fileAddr);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // 重写这部分逻辑

        int pvNum = 0;
        int pvTotalNum = task.getParms().getPvs().size();
        int totalRetrievalTimes = (int) Math.ceil((task.getParms().getTo().getTime() - task.getParms().getFrom().getTime()) /
                (SiteConfigUtil.getTimeSlotForDownload() * 1000.0d));
        int totalTimes = pvTotalNum * totalRetrievalTimes;

        for (String pvName : retrieveParms.getPvs()) {
            // 每一个PV单独来下载

            RetrieveParms retrieveParmsForThisTime = retrieveParms.clone();


            ExportUtil.exportHeader(file, pvName, retrieveParms.getFrom() + "", retrieveParms.getTo() + "");
            timestampPosition = retrieveParms.getFrom();

            int localTimes = 0;
            List<RetrieveData> retrieveDataList = null;
            while (timestampPosition.before(retrieveParms.getTo()) && task.getState() != DownloadState.Canceled) {
                Timestamp start = timestampPosition;
                long ms = timestampPosition.getTime() + SiteConfigUtil.getTimeSlotForDownload() * 1000;
                Timestamp end = new Timestamp(ms);
                if (end.after(retrieveParms.getTo())) {
                    end = retrieveParms.getTo();
                }
                timestampPosition = end;
                retrieveParmsForThisTime.setFrom(start);
                retrieveParmsForThisTime.setTo(end);
                List<Future<PVDataFromStore>> futures = new ArrayList<>();
                HashMap<String, RetrieveData> pvDataMap = new HashMap<>();
                retrieveService.startDataRetrieval(pvName, retrieveParmsForThisTime, true, futures);

                for (Future<PVDataFromStore> future : futures) {
                    try {
                        retrieveService.addPVData(future.get(), pvDataMap);
                    } catch (Exception ex) {
                        logger.error("Cannot get data from thread pool.", ex);
                    }
                }
                for (RetrieveData retrieveData : pvDataMap.values()) {
                    ExportUtil.exportSinglePVData(file, retrieveData, retrieveParms, pvName);
                }
                localTimes++;

                int progress = (((pvNum * totalRetrievalTimes) + localTimes) * 100 / totalTimes);

                task.updateProgress(progress);
            }
            pvNum++;
        }
        if (task.getState() == DownloadState.Downloading) {
            task.setState(DownloadState.Finished);
        }
        return filename;

    }

//
//    @ApiOperation("开始下载数据，返回保存数据的文件名")
//    @GetMapping("/startDownload2/{taskid}")
//    public String startDownload2(@PathVariable("taskid") String taskid) throws IOException {
//        logger.info("start download task:" + taskid);
//        DownloadTask task = downloadService.getTask(taskid);
//        if (task == null || task.getState() != DownloadState.Created) {
//            logger.info("Download task " + taskid + " not existed!");
//            return null;
//        }
//        String pvs = "";
//        for (String pv : task.getParms().getPvs()) {
//            if (!StringUtils.isEmpty(pvs)) {
//                pvs += ",";
//            }
//            pvs += pv;
//        }
//        task.setState(DownloadState.Downloading);
//        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss");
//        Date date = new Date();
//        String filename = simpleDateFormat.format(date) + ".txt";
//        String fileAddr = ConfigUtil.getConfigFilesDir() + "/records/" + filename;
//        File file = new File(fileAddr);
//        if (!file.exists()) {
//            try {
//                file.createNewFile();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//        retrieveParms = task.getParms();
//        if (retrieveService == null || retrieveParms == null) {
//            logger.error(MessageFormat.format("The retrieveService==null?{0} or RetrieveParms==null?{1} passed in is null!",
//                    retrieveService == null, retrieveParms == null));
//            return null;
//        }
//        if (retrieveParms.getFrom() == null || retrieveParms.getTo() == null) {
//            logger.error(MessageFormat.format("The retrieveParms.getFrom()==null?{0} or " +
//                            "retrieveParms.getTo()==null?{1} passed in is null!",
//                    retrieveParms.getFrom() == null, retrieveParms.getTo() == null));
//            return null;
//        }
//        StringBuilder headStr = new StringBuilder().append("Timestamp");
//        if (retrieveParms.getPvs() != null) {
//            for (String pv : retrieveParms.getPvs()) {
//                headStr.append(",").append(pv);
//                pvNameList.add(pv);
//            }
//        }
//        ExportUtil.exportHeader(file, headStr.toString());
//        timestampPosition = retrieveParms.getFrom();
//        calculateTotalSize(headStr.toString());
////            float totalSeconds = (retrieveParms.getTo().getTime() - retrieveParms.getFrom().getTime()) / 1000;
////            float maxEventNumber = 0;
////            float eventValueSizeOfOneRaw = "2075/07/05 19:19:19.196".length();
//        List<RetrieveData> retrieveDataList = null;
//        RetrieveParms retrieveParmsForThisTime = retrieveParms.clone();
//        int totalRetrievalCount = (int) Math.ceil((task.getParms().getTo().getTime() - task.getParms().getFrom().getTime()) /
//                (SiteConfigUtil.getTimeSlotForDownload() * 1000));
//        int finishedCount = 0;
//        while (timestampPosition.before(retrieveParms.getTo()) && task.getState() != DownloadState.Canceled) {
//            Timestamp start = timestampPosition;
//            // decide the end timestamp.
//            long ms = timestampPosition.getTime() + SiteConfigUtil.getTimeSlotForDownload() * 1000;
//            Timestamp end = new Timestamp(ms);
//            if (end.after(retrieveParms.getTo())) {
//                end = retrieveParms.getTo();
//            }
//            timestampPosition = end;
//            retrieveParmsForThisTime.setFrom(start);
//            retrieveParmsForThisTime.setTo(end);
//            retrieveDataList = retrieveService.retrievePVData(retrieveParmsForThisTime);
//            ExportUtil.exportPv(file, retrieveDataList, retrieveParms, pvNameList);
//            finishedCount++;
//            int progress = (finishedCount * 100 / totalRetrievalCount);
//            logger.debug("finishedCount = " + finishedCount + ", progress = " + progress + "%");
//            task.updateProgress(progress);
//        }
//        if (task.getState() == DownloadState.Downloading) {
//            task.setState(DownloadState.Finished);
//        }
//        return filename;
//    }

//

    /**
     * This is cosylab download method
     *
     * @param request
     * @param response
     * @param taskid
     */
//
//    @GetMapping("/startDownload3")
//    public void startDownload(HttpServletRequest request, HttpServletResponse response, String taskid) {
//        logger.info("Start download task: " + taskid);
//        DownloadTask task = downloadService.getTask(taskid);
//
//        if (task == null || task.getState() != DownloadState.Created) {
//            logger.info("Download task " + taskid + " not existed.");
//            return;
//        }
//        String pvs = "";
//        for (String pv : task.getParms().getPvs()) {
//            if (!StringUtils.isEmpty(pvs)) {
//                pvs += ", ";
//            }
//            pvs += pv;
//        }
////        recordUserLogService.logOperation(OperationType.DOWNLOAD_RAW_DATA, pvs);
//        task.setState(DownloadState.Downloading);
//        //String filename = "download.csv";
//        String filename = "download.txt";
//        response.setHeader("Content-Disposition", "attachment;filename=" + filename);
//        //response.setContentType("text/csv");
//        response.setContentType("txt");
//
//        try {
//            ServletOutputStream out = response.getOutputStream();
//            BufferedRetrieveService in = new BufferedRetrieveService(retrieveService, task.getParms());
//            /*
//            long length = in.getTransferSize();
//            if (length <= Integer.MAX_VALUE) {
//                response.setContentLength((int)length);
//            } else {
//                response.addHeader("Content-Length", Long.toString(length));
//            }
//            */
//            String dataStr;
//            int totalRetrievalCount = (int) Math.ceil((task.getParms().getTo().getTime() - task.getParms().getFrom().getTime()) /
//                    (SiteConfigUtil.getTimeSlotForDownload() * 1000.0d));
//            logger.debug("totalRetrievalCount = " + totalRetrievalCount);
//            int finishedCount = 0;
//            while ((dataStr = in.read()) != null) {
//                byte[] b = dataStr.getBytes(StandardCharsets.UTF_8);
//                out.write(b, 0, b.length);
//                out.write(0x0A);
//                finishedCount++;
//                int progress = (finishedCount * 100 / totalRetrievalCount);
//                logger.debug("finishedCount = " + finishedCount + ", progress = " + progress + "%");
//                if (!task.updateProgress(progress)) {
//                    // Already canceled by remote
//                    // break;
//                }
//            }
//            out.flush();
//            out.close();
//            if (task.getState() == DownloadState.Downloading) {
//                task.setState(DownloadState.Finished);
//            }
//        } catch (Exception ex) {
//            logger.error(ex);
//            task.setState(DownloadState.Terminated);
//        }
//    }

    @ApiOperation(value = "根据文件名进行下载")
    @GetMapping(value = "/media/{name}")
    public ResponseEntity<InputStreamResource> downloadFile(@PathVariable(value = "name", required = true) String name)
            throws IOException {
        FileSystemResource file = new FileSystemResource(ConfigUtil.getConfigFilesDir() + "/records/" + name);
        HttpHeaders headers = new HttpHeaders();
        headers.add("Cache-Control", "no-cache, no-store, must-revalidate");
        headers.add("Content-Disposition", name);
        headers.add("Pragma", "no-cache");
        headers.add("Expires", "0");

        return ResponseEntity
                .ok()
                .headers(headers)
                .contentLength(file.contentLength())
                .contentType(MediaType.parseMediaType("application/octet-stream"))
                .body(new InputStreamResource(file.getInputStream()));
    }

    @ApiOperation(value = "查询下载进度")
    @GetMapping("/downloadProgress/{taskId}")
    public ReturnWrap downloadProgress(@PathVariable("taskId") String taskId) {
        DownloadTask task = downloadService.getTask(taskId);
        if (task == null) {
            return new ReturnWrap(false, "Download task is not existed.");
        }
        JSONObject data = new JSONObject();
        data.put("state", task.getState().toString());
        data.put("progress", task.getProgress());
        task.setLastActiveTime(new Date());
        return new ReturnWrap(true, data);
    }

    @ApiOperation("取消下载")
    @GetMapping("/cancelDownload/{taskId}")
    @ResponseBody
    public ReturnWrap cancelDownload(@PathVariable("taskId") String taskId) {
        DownloadTask task = downloadService.getTask(taskId);
        if (task == null) {
            return new ReturnWrap(false, "Download task is not existed.");
        } else if (task.getState() != DownloadState.Downloading) {
            return new ReturnWrap(false, "Download task is already completed or aborted.");
        }
        task.setState(DownloadState.Canceled);
        return new ReturnWrap();
    }


    public static JSONObject estimateDownload(long size) {
        long byteInG = 1024 * 1024 * 1024;
        double usedTime = 1.0 * size / SiteConfigUtil.getPvDataDownloadSpeed();
        String sizeStr = "";
        if (size >= byteInG) {
            sizeStr = new DecimalFormat("0.00GB").format(1.0 * size / byteInG);
        } else {
            sizeStr = new DecimalFormat("0.00MB").format(1.0 * size / (1024 * 1024));
        }
        String timeStr = "";
        if (usedTime >= 60 * 60) {
            timeStr = new DecimalFormat("0.0 hours").format(usedTime / (60 * 60));
        } else if (usedTime >= 60) {
            timeStr = new DecimalFormat("0.0 minutes").format(usedTime / 60);
        } else {
            timeStr = new DecimalFormat("0.0 seconds").format(usedTime);
        }
        JSONObject result = new JSONObject();
        result.put("szie", sizeStr);
        result.put("time", timeStr);
        return result;
    }


    private void calculateTotalSize(String headStr) {
        /**
         * Timestamp,Linac100:PS2:QM5:COUNTER,Linac100:PS2:QM5:AI
         * 2019/05/06 16:19:24.196,3458482.902072066,3458482.902072066
         */
        float totalSeconds = (this.retrieveParms.getTo().getTime() - this.retrieveParms.getFrom().getTime()) / 1000;
        float maxEventNumber = 0;
        float eventValueSizeOfOneRaw = "2075/07/05 19:19:19.196".length();
        for (String pv : this.retrieveParms.getPvs()) {
            float eventRate = this.retrieveService.getEventRate(pv);
            if (eventRate == 0) {
                eventRate = 1.0f;
            }
            float totalEventForPV = totalSeconds * eventRate;
            // each PV has different number of events. The max is used as the number of events for all PVs.
            maxEventNumber = maxEventNumber > totalEventForPV ? maxEventNumber : totalEventForPV;

            // plus 1 because the split char (",").
            eventValueSizeOfOneRaw += this.retrieveService.calculateEventValueSize(pv) + 1;
        }

        // plus 1 because the new line char.
        totalSize = (long) (headStr.length() + 1 + (maxEventNumber * (eventValueSizeOfOneRaw + 1)));
    }


}































