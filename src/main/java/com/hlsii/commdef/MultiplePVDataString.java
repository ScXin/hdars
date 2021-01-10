package com.hlsii.commdef;

/**
 * @author Shangcong Xin
 * @date 4/1/20
 */

import com.alibaba.fastjson.JSONObject;
import com.hlsii.service.RetrieveServiceImplementation;
import org.apache.log4j.Logger;

import java.sql.Timestamp;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * This implements the PV data string download format: Timestamp,pv1,pv2,pv3...
 */
public class MultiplePVDataString {
    private static Logger logger = Logger.getLogger(RetrieveServiceImplementation.class.getName());

    private long milliSecond;
    private long second;
    private long nanos;
    private ArrayList<String> pvNameList;
    private HashMap<String, String> pvDataHashMap = new HashMap<>();
    private PVDataFormat pvDataFormat;


    public MultiplePVDataString(ArrayList<String> pvNameList, PVDataFormat pvDataFormat) {
        if (pvNameList == null || pvNameList.isEmpty()) {
            logger.error(MessageFormat.format("pvNameList {0} is not valid.", pvNameList == null? "null":"empty"));
        }
        this.pvNameList = pvNameList;
        this.pvDataFormat = pvDataFormat;
    }

    public long getmilliSecond() {
        return milliSecond;
    }
    public long getSecond() {
        return second;
    }
    public long getNanos() {
        return nanos;
    }

    public static Long getKey(PVDataFormat pvDataFormat, JSONObject jsonObject) {
        if (jsonObject == null) {
            logger.error("jsonObject is null, return null.");
            return null;
        }

        Long ms = null;
        if (pvDataFormat == PVDataFormat.QW) {
            try {
                ms = Long.parseLong(jsonObject.getString("millis"));
            } catch (NumberFormatException ex) {
                logger.error(MessageFormat.format("millis {0} is not valid.", jsonObject.getString("millis")));
            }
        } else if (pvDataFormat == PVDataFormat.JSON) {
            try {
                long secs = Long.parseLong(jsonObject.getString("secs"));
                long nanos = Long.parseLong(jsonObject.getString("nanos"));
                ms = secs*1000 +  nanos/1000000;
            } catch (NumberFormatException ex) {
                logger.error(MessageFormat.format("secs {0} or nanos {1} is not valid.",
                        jsonObject.getString("secs"), jsonObject.getString("nanos")));
            }
        }
        return ms;
    }

    /**
     * Add a PV data val into the string.
     *
     * @param pvName
     *          the PV name.
     * @param jsonObject
     *          the JSON object include time and val.
     */
    public void addPVData(String pvName, JSONObject jsonObject) {
        if (pvName == null || jsonObject == null) {
            logger.error(MessageFormat.format("pvName==null?{0} or jsonObject==null?{1}",
                    pvName == null, jsonObject == null));
            return;
        }

        if (this.pvDataFormat == PVDataFormat.QW) {
            try {
                long ms = Long.parseLong(jsonObject.getString("millis"));
                if (milliSecond == 0) {
                    milliSecond = ms;
                }
                if (milliSecond == ms) {
                    String valJS = jsonObject.getString("val");
                    pvDataHashMap.put(pvName, valJS==null? "null":valJS);
                } else {
                    logger.error(MessageFormat.format("millis {0} does NOT match jsonObject(millis) {1}.",
                            milliSecond, ms));
                }
            } catch (NumberFormatException ex) {
                logger.error(MessageFormat.format("millis {0} is not valid.", jsonObject.getString("millis")));
            }

        } else if (pvDataFormat == PVDataFormat.JSON) {
            try {
                long s = Long.parseLong(jsonObject.getString("secs"));
                if (second == 0) {
                    second = s;
                }
                long n = Long.parseLong(jsonObject.getString("nanos"));
                if (nanos == 0) {
                    nanos = n;
                }
                if (second == s && nanos == n) {
                    String valJS = jsonObject.getString("val");
                    pvDataHashMap.put(pvName, valJS==null? "null":valJS);
                } else {
                    logger.error(MessageFormat.format("second {0} does NOT match jsonObject(secs) {1}. or " +
                            "nanos {2} does NOT match jsonObject(nanos) {3}.", second, s, nanos, n));
                }
            } catch (NumberFormatException ex) {
                logger.error(MessageFormat.format("secs {0} or nanos {1} is not valid.",
                        jsonObject.getString("secs"), jsonObject.getString("nanos")));
            }

        }
    }

    /**
     * Construct multiple PV data as the format: Timestamp,pv1,pv2,pv3...
     *
     * @return the data string.
     */
    @Override
    public String toString() {
        if (pvNameList == null || pvNameList.isEmpty()) {
            logger.error(MessageFormat.format("pvNameList {0} is not valid. return empty string",
                    pvNameList == null? "null":"empty"));
            return "";
        }

        // generate timestamp string
        long ms = 0;
        if (pvDataFormat == PVDataFormat.QW) {
            ms = milliSecond;
        } else if (pvDataFormat == PVDataFormat.JSON) {
            ms = second*1000 +  nanos/1000000;
        } else {
            logger.error(MessageFormat.format("pvDataFormat {0} is not supported. return empty string", pvDataFormat));
            return "";
        }
        Timestamp ts= new Timestamp(ms);

        // build multiple PV data string.
        StringBuilder dataStr = new StringBuilder();
        dataStr.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(ts));
        for (String pv : pvNameList) {
            String valStr = pvDataHashMap.get(pv);
            dataStr.append(",").append(valStr==null?"null":valStr);    // 这一行非常关键
        }
        return dataStr.toString();
    }
}