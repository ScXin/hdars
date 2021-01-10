package com.hlsii.service;

/**
 * @author Shangcong Xin
 * @date 4/1/20
 */

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hlsii.commdef.PVDataFromStore;
import com.hlsii.commdef.PVDataStore;
import com.hlsii.commdef.RetrieveParms;
import com.hlsii.util.PoorMansProfiler;
import com.hlsii.vo.RetrieveData;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.springframework.stereotype.Service;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;

/**
 * Implement PV data multiple threads retrieval service.
 */
@Service
public class RetrieveServiceMultipleThreads extends RetrieveServiceImplementation {
    private static Logger logger = Logger.getLogger(RetrieveServiceMultipleThreads.class.getName());

    private ExecutorService threadPool;

    public RetrieveServiceMultipleThreads() {
        // create thread pool
        threadPool = new ThreadPoolExecutor(2, 16, 0, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(512), new ThreadPoolExecutor.DiscardPolicy());
    }

    @Override
    public List<RetrieveData> retrievePVData(RetrieveParms parm) {
        PoorMansProfiler poorMansProfiler = new PoorMansProfiler();

        List<RetrieveData> retrieveDataList = retrieveMultiplePVDataInThread(parm, true);

        poorMansProfiler.mark("Completed");
        retrievalMetrics.addRetrievalMetrics(PVDataStore.HADARS, poorMansProfiler.totalTimeMS(), retrieveDataList);

        return retrieveDataList;
    }

    @Override
    public List<RetrieveData> downloadPVData(RetrieveParms parm) {
        return retrieveMultiplePVDataInThread(parm, false);
    }

    private List<RetrieveData> retrieveMultiplePVDataInThread(RetrieveParms parm, boolean enableHBaseCache) {
        // valid parameters
        if (!RetrieveParms.check(parm)) {
            return new ArrayList<>();
        }

        logger.debug("Start retrieval.");

        List<Future<PVDataFromStore>> futures = new ArrayList<>();
        HashMap<String, RetrieveData> pvDataMap = new HashMap<>();

        // Iterate all PVs
        for (String pv : parm.getPvs()) {
            // Start thread to retrieve data from both AA and Hadoop.
            startDataRetrieval(pv, parm, enableHBaseCache, futures);
        }
        // get all retrieval result.
        for (Future<PVDataFromStore> future : futures) {
            try {
                addPVData(future.get(), pvDataMap);
            } catch (Exception ex) {
                logger.error("Cannot get data from thread pool.", ex);
            }
        }

        logger.debug("Complete retrieval.");

        return new ArrayList<>(pvDataMap.values());
    }

    /**
     * Add retrieval data into the HashMap.
     *
     * @param pvDataFromStore
     *          the {@link PVDataFromStore}
     */
    @Override
    public void addPVData(PVDataFromStore pvDataFromStore, HashMap<String, RetrieveData> pvDataMap) {
        if (pvDataFromStore == null) {
            logger.info("pvDataFromStore is Null!");
            return;
        }

        RetrieveData existingRetrieveData = pvDataMap.get(pvDataFromStore.getPvName());
        RetrieveData newRetrieveData = pvDataFromStore.getRetrieveData();
        if (existingRetrieveData == null) {
            if (newRetrieveData != null) {
                pvDataMap.put(pvDataFromStore.getPvName(), newRetrieveData);
            }
            return;
        }

        // Data from HADOOP is treated as the base.
        if (pvDataFromStore.getDataStore() == PVDataStore.HADOOP) {
            if (newRetrieveData != null) {
                newRetrieveData.addData(existingRetrieveData.getData());
                // in case one of meta data is null.
                if (newRetrieveData.getMeta() == null) {
                    newRetrieveData.setMeta(existingRetrieveData.getMeta());
                }
                pvDataMap.put(pvDataFromStore.getPvName(), newRetrieveData);
            }
        } else {
            existingRetrieveData.addData(newRetrieveData.getData());
            // in case one of meta data is null.
            if (existingRetrieveData.getMeta() == null) {
                existingRetrieveData.setMeta(newRetrieveData.getMeta());
            }
        }
    }

    /**
     * Retrieve PV data according to the first known event in the Hadoop.
     *
     * @param pvName
     *          the PV name.
     * @param parm
     *          the {@link RetrieveParms}
     * @param enableHBaseCache
     *          indicate whether enable HBase cache.
     * @return
     *          the {@link RetrieveData}
     */
    @Override
    public void startDataRetrieval(String pvName, RetrieveParms parm, boolean enableHBaseCache,
                                    List<Future<PVDataFromStore>> futures) {
        try {
            // Get first known event in Hadoop in order to identify the data storage.
            Event firstKnownEventInHadoop = null;
            try {
                firstKnownEventInHadoop = getHadoopRetrieveService().getFirstKnownEvent(pvName);
            } catch (Exception ex) {
                // Not able to get the first known event in Hadoop, just log, continue to get data from both AA and hadoop.
                logger.error(MessageFormat.format("Exception on getting the first known event in Hadoop for PV {0}.",
                        pvName));
            }

            List<PVDataStore> dataStores = getAaRetrieveService().resolveDataStore(pvName, parm.getFrom(), parm.getTo(),
                    firstKnownEventInHadoop);

            if(dataStores.contains(PVDataStore.AA)) {
                // Get PV data from AA
                Callable ca = () -> {
                    PoorMansProfiler poorMansProfiler = new PoorMansProfiler();
                    PVDataFromStore pvDataFromStore = getAaRetrieveService().getData(pvName, parm);
                    poorMansProfiler.mark("Completed");
                    retrievalMetrics.addRetrievalMetrics(poorMansProfiler.totalTimeMS(), pvDataFromStore);
                    return pvDataFromStore;
                };
                Future f = threadPool.submit(ca);
                futures.add(f);
            }


//            System.out.println("hahha");
            if(dataStores.contains(PVDataStore.HADOOP)) {
                // Get PV data from Hadoop
                Callable ca = () -> {
                    PoorMansProfiler poorMansProfiler = new PoorMansProfiler();
                    PVDataFromStore pvDataFromStore = retrieveFromHadoop(pvName, parm, enableHBaseCache);
                    poorMansProfiler.mark("Completed");
                    retrievalMetrics.addRetrievalMetrics(poorMansProfiler.totalTimeMS(), pvDataFromStore);
                    return pvDataFromStore;
                };
                Future f = threadPool.submit(ca);
                futures.add(f);
            }
        } catch (Exception ex) {
            logger.error("Exception on retrieving PV data", ex);
        }
    }

    /**
     * Retrieve data from Hadoop.
     *
     * @param pvName
     *          the PV name.
     * @param parm
     *          the {@link RetrieveParms}
     * @param enableHBaseCache
     *          indicate whether enable HBase cache.
     * @return
     *          the {@link PVDataFromStore}
     */
    private PVDataFromStore retrieveFromHadoop(String pvName, RetrieveParms parm, boolean enableHBaseCache) {
        if (getHadoopRetrieveService() == null) {
            logger.error("hadoopRetrieveService is not initialized yet.");
            return null;
        }
        PoorMansProfiler poorMansProfiler = new PoorMansProfiler();

        JSONArray dataArray = retrievePVDataFromHadoop(pvName, parm, enableHBaseCache);
        JSONObject metaData = retrievePVMetaDataFromHadoop(pvName);

        poorMansProfiler.mark("After Hadoop retrieval.");

        logger.info(MessageFormat.format("Get data for PV {0} from Hadoop, total {1}ms",
                pvName, poorMansProfiler.totalTimeMS()));

        if (dataArray == null && metaData == null) {
            logger.error(MessageFormat.format("return null for PV {0} because no PV data and meta data from Hadoop.",
                    pvName));
            return null;
        }

        return new PVDataFromStore(pvName, PVDataStore.HADOOP, new RetrieveData(pvName, metaData, dataArray));
    }

}