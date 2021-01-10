package com.hlsii.service;

/**
 * @author Shangcong Xin
 * @date 4/1/20
 */

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hlsii.commdef.MultiplePVDataString;
import com.hlsii.commdef.RetrieveParms;
import com.hlsii.util.SiteConfigUtil;
import com.hlsii.vo.RetrieveData;
import org.apache.log4j.Logger;

import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Implement buffered PV data retrieval service.
 */
public class BufferedRetrieveService {
    private static Logger logger = Logger.getLogger(BufferedRetrieveService.class.getName());

    private IRetrieveService retrieveService;

    private RetrieveParms retrieveParms;
    private LinkedBlockingDeque<String> queue;
    private Timestamp timestampPosition;
    private ArrayList<String> pvNameList = new ArrayList<>();

    private long totalSize = 0;

    /**
     * Constructor
     *
     * @param retrieveParms
     *          the {@link RetrieveParms}
     */
    public BufferedRetrieveService(IRetrieveService retrieveService, RetrieveParms retrieveParms) {
        if (retrieveService == null || retrieveParms == null) {
            logger.error(MessageFormat.format("The retrieveService==null?{0} or RetrieveParms==null?{1} passed in is null!",
                    retrieveService == null, retrieveParms==null));
            return;
        }

        if (retrieveParms.getFrom() == null || retrieveParms.getTo() == null) {
            logger.error(MessageFormat.format("The retrieveParms.getFrom()==null?{0} or " +
                            "retrieveParms.getTo()==null?{1} passed in is null!",
                    retrieveParms.getFrom() == null, retrieveParms.getTo()==null));
            return;
        }

        this.retrieveService = retrieveService;
        this.retrieveParms = retrieveParms;
        queue = new LinkedBlockingDeque<>();

        StringBuilder headStr = new StringBuilder().append("Timestamp");
        if (this.retrieveParms.getPvs() != null) {
            for (String pv : this.retrieveParms.getPvs()) {
                headStr.append(",").append(pv);
                pvNameList.add(pv);
            }
            queue.add(headStr.toString());
        }

        timestampPosition = this.retrieveParms.getFrom();

        calculateTotalSize(headStr.toString());
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

    /**
     * Read a line of data
     *
     * @return
     *          A String of the line of data.
     */
    public String read() {
        if (queue == null) {
            // queue is null, something is wrong.
            logger.error("Queue is null. The RetrieveParms is not valid in the construct function.");
            return null;
        }

        if (queue.isEmpty()) {
            if (timestampPosition == null || this.retrieveParms.getTo() == null) {
                logger.error(MessageFormat.format("RetrieveParms.getFrom()==null?{0} or RetrieveParms.getTo()==null?{1}",
                        this.retrieveParms.getFrom() == null, this.retrieveParms.getTo() == null));
                return null;
            }
            produceData();
        }

        try {
            return queue.remove();
        } catch (NoSuchElementException ex) {
            logger.debug("All PV data are already read.");
            return null;
        }
    }

    /**
     * Retrieve PV data, construct the result as string ([timestamp,pv1,pv2,pv3...]) and put to the queue.
     * Note: This is the producer in the producer/consumer model. It would be optimized to be a thread.
     * Now, keep it simple.
     */
    private void produceData() {
        List<RetrieveData> retrieveDataList = null;
        RetrieveParms retrieveParmsForThisTime = retrieveParms.clone();

        while (true) {
            if (!timestampPosition.before(retrieveParms.getTo())) {
                // all data are gotten, return.
                return;
            }

            Timestamp start = timestampPosition;

            // decide the end timestamp.
            long ms = timestampPosition.getTime() + SiteConfigUtil.getTimeSlotForDownload() * 1000;
            Timestamp end = new Timestamp(ms);
            if (end.after(retrieveParms.getTo())) {
                end = retrieveParms.getTo();
            }
            timestampPosition = end;

            retrieveParmsForThisTime.setFrom(start);
            retrieveParmsForThisTime.setTo(end);

            // retrieve data from AA and Hadoop.
            retrieveDataList = retrieveService.downloadPVData(retrieveParmsForThisTime);
            boolean haveData = false;
            if (retrieveDataList != null && !retrieveDataList.isEmpty()) {
                for (RetrieveData retrieveData : retrieveDataList) {
                    if (retrieveData.getData() != null && !retrieveData.getData().isEmpty()) {
                        haveData = true;
                        break;
                    }
                }
            }

            if (haveData) {
                // when PV data is gotten, break
                break;
            }
        }

        // Parse result
        TreeMap<Long, MultiplePVDataString> pvDataStringTreeMap = new TreeMap<>();
        for (RetrieveData retrieveData : retrieveDataList) {
            /*if (retrieveData.getMeta() == null) {
                continue;
            }*/
            String pvName = retrieveData.getPvName();

            JSONArray dataArray = retrieveData.getData();
            for (Object o : dataArray) {
                JSONObject jsonObject = (JSONObject) o;
                Long key = MultiplePVDataString.getKey(this.retrieveParms.getPvDataFormat(), jsonObject);
                if (key != null) {
                    MultiplePVDataString pvDataString = pvDataStringTreeMap.get(key);
                    if (pvDataString == null) {
                        pvDataString = new MultiplePVDataString(this.pvNameList, this.retrieveParms.getPvDataFormat());
                        pvDataStringTreeMap.put(key, pvDataString);
                    }
                    pvDataString.addPVData(pvName, jsonObject);
                }
            }
        }

        if (!pvDataStringTreeMap.isEmpty()) {
            // add PV data string into the queue
            for (Object aSet : pvDataStringTreeMap.entrySet()) {
                Map.Entry me = (Map.Entry) aSet;
                this.queue.add(me.getValue().toString());
            }
        }
    }

    public long getTotalSize() {
        return totalSize;
    }


    public long getTransferSize() {
        return (long) (totalSize * retrieveParms.getPvs().size() *
                SiteConfigUtil.getPvDataDownloadTransferLengthCalculationFactor());
    }
}