package com.hlsii.service;

/**
 * @author shxin
 * @date 2021/1/10
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
public class BufferedRetrieveService2 {
    private static Logger logger = Logger.getLogger(BufferedRetrieveService.class.getName());

    private IRetrieveService retrieveService;

    private RetrieveParms retrieveParms;
    //    private LinkedBlockingDeque<String> queue;
//    private Timestamp timestampPosition;
    private ArrayList<String> pvNameList = new ArrayList<>();

    private long totalSize = 0;

    /**
     * Constructor
     *
     * @param retrieveParms the {@link RetrieveParms}
     */
    public BufferedRetrieveService2(IRetrieveService retrieveService, RetrieveParms retrieveParms) {
        if (retrieveService == null || retrieveParms == null) {
            logger.error(MessageFormat.format("The retrieveService==null?{0} or RetrieveParms==null?{1} passed in is null!",
                    retrieveService == null, retrieveParms == null));
            return;
        }

        if (retrieveParms.getFrom() == null || retrieveParms.getTo() == null) {
            logger.error(MessageFormat.format("The retrieveParms.getFrom()==null?{0} or " +
                            "retrieveParms.getTo()==null?{1} passed in is null!",
                    retrieveParms.getFrom() == null, retrieveParms.getTo() == null));
            return;
        }

        this.retrieveService = retrieveService;
        this.retrieveParms = retrieveParms;
//        queue = new LinkedBlockingDeque<>();
        calculateTotalSize();
//        timestampPosition = this.retrieveParms.getFrom();
    }

    private void calculateTotalSize() {
        /**
         * Timestamp,Linac100:PS2:QM5:COUNTER,Linac100:PS2:QM5:AI
         * 2019/05/06 16:19:24.196,3458482.902072066,3458482.902072066
         */
        float totalSeconds = (this.retrieveParms.getTo().getTime() - this.retrieveParms.getFrom().getTime()) / 1000;

        float eventValueSizeOfOneRaw = "2075-07-05 19:19:19.196".length();
        for (String pv : this.retrieveParms.getPvs()) {
            float eventRate = this.retrieveService.getEventRate(pv);
            if (eventRate == 0) {
                eventRate = 1.0f;
            }
            float totalEventForPV = totalSeconds * eventRate;
            // each PV has different number of events. The max is used as the number of events for all PVs.

            // plus 1 because the split char (",").
            totalSize += (long) (this.retrieveService.calculateEventValueSize(pv) + 2 + eventValueSizeOfOneRaw) * totalEventForPV;
        }
        // plus 1 because the new line char.

    }


    public long getTotalSize() {
        return totalSize;
    }


    public long getTransferSize() {
        return (long) (totalSize * retrieveParms.getPvs().size() *
                SiteConfigUtil.getPvDataDownloadTransferLengthCalculationFactor());
    }
}
