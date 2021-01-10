package com.hlsii.service;

//import com.hlsii.commdef.PostProcessing;

import com.hlsii.commdef.PVDataFromStore;
import hadarshbaseplugin.commdef.PostProcessing;
import com.hlsii.commdef.RetrieveParms;
import com.hlsii.vo.RetrieveData;
import com.hlsii.vo.StatisticsData;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * Data Retrieve Service interface
 */
public interface IRetrieveService {
    /**
     * Retrieve data for the specified PVs
     *
     * @param parm - retrieve parameters
     * @return - Retrieve data
     */
    List<RetrieveData> retrievePVData(RetrieveParms parm);

    /**
     * Download data for the specified PVs
     *
     * @param parm - retrieve parameters
     * @return - Download data
     */
    List<RetrieveData> downloadPVData(RetrieveParms parm);

    /**
     * Get the down sampling interval (seconds) for the time span between the startTime and endTime of the given down
     * sampling type.
     *
     * @param pvName               - the PV name.
     * @param downSamplingIdentify - down sampling type.
     * @param startTime            - the start time.
     * @param endTime              - then end time.
     * @return the value of down sampling interval (seconds). The return value is between 0 to 65535. For the down
     * sampling type NONE the return value is 0.
     */
    int getSamplingInterval(String pvName, PostProcessing downSamplingIdentify, Timestamp startTime, Timestamp endTime);

    /**
     * Get the PV event rate (events/second).
     *
     * @param pvName the PV name.
     * @return
     */
    float getEventRate(String pvName);

    /**
     * Get the PV sample period (second).
     *
     * @param pvName the PV name.
     * @return
     */
    float getSamplePeriod(String pvName);

    /**
     * Calculate the size when PV event value is translated to a string.
     *
     * @param pvName the PV name.
     * @return
     */
    long calculateEventValueSize(String pvName);

    /**
     * Get the statistical information for a PV
     *
     * @param pvName    - the PV name
     * @param startTime - the start time.
     * @param endTime   - the end time
     * @return state information
     * @throws IOException if access HBase failed.
     */
    StatisticsData getPVStat(String pvName, Timestamp startTime, Timestamp endTime) throws IOException;

    /**
     * Get available AA (ip:port)
     *
     * @return a set of {@link String}, or null if any exception.
     */
    Set<String> getAvailableAA();


    void startDataRetrieval(String pvName, RetrieveParms parm, boolean enableHBaseCache,
                            List<Future<PVDataFromStore>> futures);

    void addPVData(PVDataFromStore pvDataFromStore, HashMap<String, RetrieveData> pvDataMap);
}
