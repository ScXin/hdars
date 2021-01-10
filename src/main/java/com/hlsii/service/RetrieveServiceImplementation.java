package com.hlsii.service;

/**
 * @author Shangcong Xin
 * @date 4/1/20
 */

import cls.stat_information_plugin.StatInformation;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hlsii.commdef.PVDataFromStore;
import hadarshbaseplugin.api.IHadoopStorage;
import hadarshbaseplugin.commdef.PostProcessing;
import com.hlsii.commdef.PVDataFormat;
import com.hlsii.commdef.PVDataStore;
import com.hlsii.commdef.RetrieveParms;
import com.hlsii.metrics.IRetrievalMetrics;
import com.hlsii.util.PoorMansProfiler;
import com.hlsii.util.SiteConfigUtil;
import com.hlsii.vo.RetrieveData;
import com.hlsii.vo.StatisticsData;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

//import hadarshbaseplugin.HadoopStorageHBaseImpl;
//import hadarshbaseplugin.api.IHadoopStorage;
//import hadarshbaseplugin.commdef.PostProcessing;
//import hadarshbaseplugin.api.IHadoopStorage;
//import hadarshbaseplugin.commdef.PostProcessing;

//import com.hlsii.commdef.PostProcessing;

/**
 * Implement PV data retrieval service.
 */


public class RetrieveServiceImplementation implements IRetrieveService, IHBaseHealthService {
    private static Logger logger = Logger.getLogger(RetrieveServiceImplementation.class.getName());

    private IHadoopStorage hadoopRetrieveService;

    // Executor for the scheduled task
    ScheduledExecutorService scheduledService = Executors
            .newSingleThreadScheduledExecutor();
    // Executor for the health check task
    ScheduledExecutorService healthCheckService = Executors
            .newSingleThreadScheduledExecutor();

    @Autowired
    private IAARetrieveService aaRetrieveService;

    public IHadoopStorage getHadoopRetrieveService() {
        return hadoopRetrieveService;
    }

    public IAARetrieveService getAaRetrieveService() {
        return aaRetrieveService;
    }

    @Autowired
    IRetrievalMetrics retrievalMetrics;

    /**
     * Initialize a AARetrievalService and a HadoopRetrieveService.
     *
     * @return true is everything is ok, false if no AA configured or {@link IHadoopStorage} initialization failure.
     * @throws if initialization exception.
     */
    private void initialize() {
        boolean initAA = false;
        while (true) {
            if (initAA) {
                IHadoopStorage initHadoopStorage = initializeHBase();
                if (initHadoopStorage != null) {
                    hadoopRetrieveService = initHadoopStorage;
                    break;
                }
            } else {
                logger.info("Initialize aaRetrieveServiceImplementation.");
                try {
                    if (aaRetrieveService.initialize()) {
                        initAA = true;
                        logger.info("Initialize aaRetrieveServiceImplementation completed.");
                    } else {
                        logger.error("Error: Initializing aaRetrieveServiceImplementation failure.");
                    }
                } catch (Exception ex) {
                    logger.error("Exception on RetrieveServiceImplementation initialization.", ex);
                }
            }
        }
        logger.info("RetrieveServiceImplementation initialization is completed!");
    }

    private IHadoopStorage initializeHBase() {
        IHadoopStorage hadoopService = HadoopStorageSingleton.getHadoopStorage();
        if (hadoopService == null) {
            logger.error("Error: Initializing a IHadoopStorage implementation failure.");
        } else {
            logger.info("HadoopStorageHBaseImpl initialization is completed!");
        }
        return hadoopService;
    }

    private void reInitializeHBase() {
        logger.info("HadoopStorageHBaseImpl re-initializing ...");
        IHadoopStorage hadoopService = initializeHBase();
        if (hadoopService != null) {
            hadoopRetrieveService = hadoopService;
            logger.info("HadoopStorageHBaseImpl re-initialization is completed!");
            scheduledService.shutdown();
        }
    }

    @PostConstruct
    public void postInitialize() {
        scheduledService.schedule(() -> initialize(), 1, TimeUnit.SECONDS);
        healthCheckService.scheduleAtFixedRate(() -> monitorHBase(), 1000, SiteConfigUtil.getHbaseCheckingInterval(),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public List<RetrieveData> retrievePVData(RetrieveParms parm) {
        PoorMansProfiler poorMansProfiler = new PoorMansProfiler();

        List<RetrieveData> retrieveDataList = retrieveMultiplePVData(parm, true);

        poorMansProfiler.mark("Completed");
        retrievalMetrics.addRetrievalMetrics(PVDataStore.HADARS, poorMansProfiler.totalTimeMS(), retrieveDataList);

        return retrieveDataList;
    }

    @Override
    public List<RetrieveData> downloadPVData(RetrieveParms parm) {
        return retrieveMultiplePVData(parm, false);
    }


    private List<RetrieveData> retrieveMultiplePVData(RetrieveParms parm, boolean enableHBaseCache) {
        List<RetrieveData> retrieveDataList = new ArrayList<>();
        // valid parameters
        if (!RetrieveParms.check(parm)) {
            return retrieveDataList;
        }

        logger.debug("Start retrieval.");

        // Iterate all PVs
        for (String pv : parm.getPvs()) {
            RetrieveData retrieveData = retrieveData(pv, parm, enableHBaseCache);
            if (retrieveData != null) {
                retrieveDataList.add(retrieveData);
            }
        }

        logger.debug("Complete retrieval.");
        return retrieveDataList;
    }

    /**
     * Retrieve PV data according to the first known event in the Hadoop.
     *
     * @param pvName the PV name.
     * @param parm   the {@link RetrieveParms}
     * @return the {@link RetrieveData}
     */
    private RetrieveData retrieveData(String pvName, RetrieveParms parm, boolean enableHBaseCache) {
        try {
            // Get first known event in Hadoop in order to identify the data storage.
            Event firstKnownEventInHadoop = null;
            try {
                firstKnownEventInHadoop = hadoopRetrieveService.getFirstKnownEvent(pvName);
            } catch (Exception ex) {
                // Not able to get the first known event in Hadoop, just log, continue to get data from both AA and hadoop.
                logger.error(MessageFormat.format("Exception on getting the first known event in Hadoop for PV {0}.",
                        pvName));
            }

            List<PVDataStore> dataStores = aaRetrieveService.resolveDataStore(pvName, parm.getFrom(), parm.getTo(),
                    firstKnownEventInHadoop);

            RetrieveData retrieveData = null;
            if (dataStores.contains(PVDataStore.AA)) {
                // Get PV data from AA
                try {
                    PoorMansProfiler poorMansProfiler = new PoorMansProfiler();
                    retrieveData = aaRetrieveService.retrieveData(pvName, parm);
                    poorMansProfiler.mark("Completed");
                    retrievalMetrics.addRetrievalMetrics(PVDataStore.AA, poorMansProfiler.totalTimeMS(), retrieveData);
                } catch (Exception ex) {
                    logger.error("Exception on retrieving data from AA for PV " + pvName, ex);
                }
            }

            JSONArray longTermDataArrayFromHadoop = null;
            if (dataStores.contains(PVDataStore.HADOOP)) {
                PoorMansProfiler poorMansProfiler = new PoorMansProfiler();
                // Get PV data from Hadoop
                longTermDataArrayFromHadoop = retrievePVDataFromHadoop(pvName, parm, enableHBaseCache);
                poorMansProfiler.mark("Completed");
                retrievalMetrics.addRetrievalMetrics(PVDataStore.HADOOP, poorMansProfiler.totalTimeMS(),
                        longTermDataArrayFromHadoop);
            }

            // if both AA and hadoop return null, return null.
            if (retrieveData == null && longTermDataArrayFromHadoop == null) {
                // both AA and Hadoop return null, log
                logger.error(MessageFormat.format("Get null event for PV {0}.", pvName));
                return null;
            }

            // if AA returns null.
            if (retrieveData == null) {
                retrieveData = new RetrieveData(pvName);

                // get meta data from Hadoop.
                JSONObject object = retrievePVMetaDataFromHadoop(pvName);
                if (object != null) {
                    retrieveData.setMeta(object);
                }
            }

            retrieveData.addData(longTermDataArrayFromHadoop);

            return retrieveData;
        } catch (Exception ex) {
            logger.error("Exception on retrieving PV data", ex);
        }

        return null;
    }

    /**
     * Retrieve data from Hadoop.
     *
     * @param pvName the PV name.
     * @param parm   the {@link RetrieveParms}
     * @return
     */
    protected JSONArray retrievePVDataFromHadoop(String pvName, RetrieveParms parm, boolean enableHBaseCache) {
        if (hadoopRetrieveService == null) {
            logger.error("hadoopRetrieveService is not initialized yet.");

//            System.out.println("hadoopRetrieveService has not been initialized!");
            return null;
        }

        logger.debug(MessageFormat.format("Retrieve data for PV {0} from Hadoop.", pvName));
        JSONArray eventArray = new JSONArray();
        try {
            List<Event> eventList = hadoopRetrieveService.getData(pvName, parm.getFrom(), parm.getTo(),
                    parm.getPostProcessIdentity(), parm.getIntervalSeconds(), enableHBaseCache, getSamplePeriod(pvName));

            if (eventList == null) {
                logger.error(MessageFormat.format("Retrieve PV {0} data from Hadoop failure.", pvName));
                return null;
            }

            boolean v4Flag = aaRetrieveService.isV4(pvName);

            for (Event event : eventList) {
                if (event != null) {
                    JSONObject eventObject = Event2JSON(pvName, event, parm.getPvDataFormat(), v4Flag);
                    eventArray.add(eventObject);
                } else {
                    logger.warn("This is a null event from Hadoop for PV " + pvName);
                }
            }

        } catch (Exception ex) {
            logger.error(MessageFormat.format("Exception: Retrieve PV {0} data from Hadoop.", pvName), ex);
            eventArray = null;
        }
        logger.debug(MessageFormat.format("Get {0} event for PV {1} from Hadoop.",
                eventArray == null ? 0 : eventArray.size(), pvName));

        return eventArray;
    }

    /**
     * Retrieve data from Hadoop.
     *
     * @param pvName the PV name.
     * @return the {@link JSONObject}
     */
    protected JSONObject retrievePVMetaDataFromHadoop(String pvName) {
        if (hadoopRetrieveService == null) {
            logger.error("hadoopRetrieveService is not initialized yet.");
            return null;
        }
        JSONObject object = null;
        logger.debug(MessageFormat.format("Try to get meta data for PV {0} from Hadoop.", pvName));
        try {
            String metaStr = hadoopRetrieveService.getMeta(pvName);
            if (metaStr != null) {
                object = JSONObject.parseObject(metaStr);
                if (object == null) {
                    logger.error(MessageFormat.format(
                            "Cannot parse meta data string {0} gotten from Hadoop to JSONObject for PV {1}",
                            metaStr, pvName));
                }
            } else {
                logger.error(MessageFormat.format("Cannot get meta data from Hadoop for PV {0}", pvName));
            }

        } catch (Exception ex) {
            logger.error(MessageFormat.format("Cannot get meta data from Hadoop for PV {0}", pvName));
        }

        return object;
    }

    /**
     * Convert a Event to {@link JSONObject}
     *
     * @param pvName       the PV name.
     * @param event        the {@link Event}.
     * @param pvDataFormat the {@link PVDataFormat}.
     * @return the {@link JSONObject} or null if any exception occurs.
     */
    static JSONObject Event2JSON(String pvName, Event event, PVDataFormat pvDataFormat, boolean v4) {
        if (pvDataFormat.equals(PVDataFormat.QW)) {
            return Event2QW(pvName, event, v4);
        }
        return Event2JSONWithAll(pvName, event, v4);
    }

    /**
     * Convert a Event to {@link JSONObject} with all fields.
     *
     * @param pvName the PV name.
     * @param event  the {@link Event}.
     * @return the {@link JSONObject} or null if any exception occurs.
     */
    private static JSONObject Event2JSONWithAll(String pvName, Event event, boolean v4) {
        JSONObject eventObject = new JSONObject();
        try {
            DBRTimeEvent dbrTimeEvent = (DBRTimeEvent) event;
            eventObject.put("secs", dbrTimeEvent.getEpochSeconds());
            eventObject.put("nanos", Integer.toString(dbrTimeEvent.getEventTimeStamp().getNanos()));
            eventObject.put("severity", Integer.toString(dbrTimeEvent.getSeverity()));
            eventObject.put("status", Integer.toString(dbrTimeEvent.getStatus()));
            eventObject.put("val", convertValueString(dbrTimeEvent.getSampleValue().toJSONString(), v4));
        } catch (Exception ex) {
            logger.error(MessageFormat.format("Exception: Parse Event to JSON for PV {0}.", pvName), ex);
            eventObject = null;
        }
        return eventObject;
    }

    /**
     * Convert a Event to {@link JSONObject} with "millis" and "val". It is for the quick chart.
     *
     * @param pvName the PV name.
     * @param event  the {@link Event}.
     * @return the {@link JSONObject} or null if any exception occurs.
     */
    private static JSONObject Event2QW(String pvName, Event event, boolean v4) {
        JSONObject eventObject = new JSONObject();
        try {
            DBRTimeEvent dbrTimeEvent = (DBRTimeEvent) event;
            eventObject.put("millis", dbrTimeEvent.getEpochSeconds() * 1000 + dbrTimeEvent.getEventTimeStamp().getNanos() / 1000000);
            eventObject.put("val", convertValueString(dbrTimeEvent.getSampleValue().toJSONString(), v4));
        } catch (Exception ex) {
            logger.error(MessageFormat.format("Exception: Parse Event to JSON for PV {0}.", pvName), ex);
            eventObject = null;
        }
        return eventObject;
    }

    public static String convertValueString(String valueStr, boolean v4) {
        if (valueStr == null || valueStr.equals("NaN")) {
            return "null";
        }

        String valStr = valueStr;
        if (v4) {
            try {
                JSONObject valObject = JSON.parseObject(valueStr);
                valStr = valObject.get("value").toString().toLowerCase();
                if (valStr.equals("true")) {
                    valStr = "1";
                }
                if (valStr.equals("false")) {
                    valStr = "0";
                }
                if (valStr.contains("[") && valStr.contains("]")) {
                    if (valStr.contains("true") || valStr.contains("false")) {
                        valStr = valStr.replaceAll("true", "1").replaceAll("false", "0");
                    }
                }
            } catch (Exception ex) {
                logger.debug(MessageFormat.format("Exception on parsing valueStr: {0}", valueStr));
            }
        }

        return valStr;
    }

    @Override
    public int getSamplingInterval(String pvName, PostProcessing downSamplingIdentify, Timestamp startTime, Timestamp endTime) {


//        HadoopStorageHBaseImpl hadoopRetrieveService = new HadoopStorageHBaseImpl();
        if (hadoopRetrieveService == null) {
            initialize();
        }
//        System.out.println(hadoopRetrieveService == null);
        return hadoopRetrieveService.getSamplingInterval(downSamplingIdentify, startTime, endTime,
                getSamplePeriod(pvName));
    }

    @Override
    public float getEventRate(String pvName) {
        return aaRetrieveService.getEventRate(pvName);
    }

    @Override
    public float getSamplePeriod(String pvName) {
        return aaRetrieveService.getSamplePeriod(pvName);
    }

    @Override
    public long calculateEventValueSize(String pvName) {
        return aaRetrieveService.calculateEventValueSize(pvName);
    }

    @Override
    public StatisticsData getPVStat(String pvName, Timestamp startTime, Timestamp endTime) throws IOException {
        List<StatInformation> statSet = new ArrayList<>();
        StatInformation stat = hadoopRetrieveService.getStat(pvName, startTime, endTime);
        if (stat != null) {
            statSet.add(stat);
        }
        StatInformation aaStat = aaRetrieveService.getStat(pvName, startTime, endTime);
        if (aaStat != null) {
            statSet.add(aaStat);
        }
        double count = 0;
        double sum = 0;
        double squareSum = 0;
        double max = 0;
        double min = 0;
        for (StatInformation statInformation : statSet) {
            if (statInformation.count() != 0) {
                count += statInformation.count();
                sum += statInformation.sum();
                squareSum += statInformation.squareSum();
                max = statInformation.max() > max ? statInformation.max() : max;
                min = statInformation.min() < min ? statInformation.min() : min;
            }
        }

        if (count > 0) {
            double mean = sum / count;
            double rms = Math.sqrt(squareSum / count);
            double deviation = Math.sqrt((squareSum / count) - Math.pow(mean, 2));
            return new StatisticsData(mean, deviation, rms, max, min);
        }
        return new StatisticsData();
    }

    @Override
    public Set<String> getAvailableAA() {
        return aaRetrieveService.getAvailableAA();
    }

    @Override
    public void startDataRetrieval(String pvName, RetrieveParms parm, boolean enableHBaseCache, List<Future<PVDataFromStore>> futures) {

    }

    @Override
    public void addPVData(PVDataFromStore pvDataFromStore, HashMap<String, RetrieveData> pvDataMap) {

    }

    @Override
    public boolean hbaseInService() {
        return hadoopRetrieveService != null;
    }

    /**
     * Checking HBase health status by getting data for a PV. If there is IOException thrown,
     * it means the HBase is down, otherwise it is OK.
     * <p>
     * To get better performance for the checking, use a long time ago for checking, so that
     * there is no data for that PV on HBase, that will return faster.
     */
    private void monitorHBase() {
        if (hadoopRetrieveService == null) {
            return;
        }
        logger.info("Checking Hbase health status ...");
        List<PVTypeInfo> pvInfos = new ArrayList<>(aaRetrieveService.getAllPVInfo());
        if (pvInfos.isEmpty()) {
            logger.info("No archving PV.");
            return;
        }
        // Use the first PV for the checking
        String pv = pvInfos.get(0).getPvName();
        Calendar end = Calendar.getInstance();
        end.setTime(new Date());
        end.add(Calendar.YEAR, -10);
        Calendar start = Calendar.getInstance();
        start.setTime(end.getTime());
        start.add(Calendar.MINUTE, -30);
        try {
            hadoopRetrieveService.getData(pv, new Timestamp(start.getTime().getTime()),
                    new Timestamp(end.getTime().getTime()), PostProcessing.FIRSTSAMPLE, 60, false, 1.0);
        } catch (IOException e) {
            logger.warn("HBase is down.");
            hadoopRetrieveService = null;
            scheduledService.scheduleAtFixedRate(() -> reInitializeHBase(), 1000,
                    SiteConfigUtil.getHbaseCheckingInterval(), TimeUnit.MILLISECONDS);
        }
    }


}