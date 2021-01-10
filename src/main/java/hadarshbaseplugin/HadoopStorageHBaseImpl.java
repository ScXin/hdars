package hadarshbaseplugin;


import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import hadarshbaseplugin.api.IHadoopStorage;
import hadarshbaseplugin.commdef.CommonConstants;
import hadarshbaseplugin.commdef.DownSamplingLevels;
import hadarshbaseplugin.commdef.DownSamplingMethods;
import hadarshbaseplugin.commdef.HadarsHbaseConfiguration;
import hadarshbaseplugin.commdef.PBMeanScalarDoubleEvent;
import hadarshbaseplugin.commdef.PostProcessing;
import hadarshbaseplugin.commdef.PvInfo;
import hadarshbaseplugin.commdef.RowsForEventPBData;
import hadarshbaseplugin.commdef.RowsForEventPBData.rowData;
import hadarshbaseplugin.utilites.DownSampleUtility;
import hadarshbaseplugin.utilites.HBaseTableManager;
import hadarshbaseplugin.utilites.PB2Event;
import hadarshbaseplugin.utilites.PVIdManager;

import cls.stat_information_plugin.StatInformation;
import cls.stat_information_plugin.StatInformationPlugin;

/**
 * Implement the HadoopStorage for access the Hadoop HBase.
 */
public class HadoopStorageHBaseImpl implements IHadoopStorage {
    private Connection conn = null;
    private boolean inited = false;
    private static TableName pVDataTable;
    private static TableName pVDownSamplingDataTable;
    private static TableName pVDownSamplingJobTable;
    private static Logger logger = Logger.getLogger(HadoopStorageHBaseImpl.class.getName());
    private static PVIdManager pVIdmanager = new PVIdManager();
    private static HadarsHbaseConfiguration config;
    private static List<ArchDBRTypes> supportCalculatePVTypes = new ArrayList<>(); // Only these type support calculate
    // down sample value.
    private static StatInformationPlugin statPlugin;
    private static EnumMap<PostProcessing, DownSamplingMethods> PostProcessing2DownSamplingMethods = new EnumMap<>(
            PostProcessing.class);

    static {
        PostProcessing2DownSamplingMethods.put(PostProcessing.NONE, DownSamplingMethods.RAW); // no post processing
        PostProcessing2DownSamplingMethods.put(PostProcessing.LASTSAMPLE, DownSamplingMethods.FIRST); // select last
        // sample
        PostProcessing2DownSamplingMethods.put(PostProcessing.FIRSTSAMPLE, DownSamplingMethods.FIRST); // select first
        // sample
        PostProcessing2DownSamplingMethods.put(PostProcessing.MEAN, DownSamplingMethods.AVERAGE); // use the average
        // value of the
        // samples
        PostProcessing2DownSamplingMethods.put(PostProcessing.MIN, DownSamplingMethods.MIN); // use the min value of the
        // samples
        PostProcessing2DownSamplingMethods.put(PostProcessing.MAX, DownSamplingMethods.MAX); // use the max value of the
        // samples
        supportCalculatePVTypes.add(ArchDBRTypes.DBR_SCALAR_BYTE);
        supportCalculatePVTypes.add(ArchDBRTypes.DBR_SCALAR_DOUBLE);
        supportCalculatePVTypes.add(ArchDBRTypes.DBR_SCALAR_FLOAT);
        supportCalculatePVTypes.add(ArchDBRTypes.DBR_SCALAR_INT);
        supportCalculatePVTypes.add(ArchDBRTypes.DBR_SCALAR_SHORT);
    }

    public static List<ArchDBRTypes> getSupportCalculatePVTypes() {
        return supportCalculatePVTypes;
    }

    public HadoopStorageHBaseImpl() {
        // constructor
    }

    @Override
    public String getName() {
        return "HadoopStorageHBaseImpl";
    }

    /**
     * Initialize the storage. e.g. for HBase, set up connection.
     *
     * @param configFile - a path of configuration file.
     * @return true is everything is ok, false if any error.
     * @throws IOException if access HBase failed.
     */
    @Override
    public boolean initialize(String configFile) throws IOException {

//        System.out.println("111");
        File f = new File(configFile);
        InputStream confFile = new FileInputStream(f);
        boolean result = initialize(confFile);
        confFile.close();
        return result;
    }

    /**
     * Initialize the storage. e.g. for HBase, set up connection.
     *
     * @param configFileStream The input steam of configuration file.
     * @return true is everything is ok, false if any error.
     * @throws IOException
     */
    @Override
    public boolean initialize(InputStream configFileStream) throws IOException {

//        System.out.println("456");
        config = HadarsHbaseConfiguration.fromStream(configFileStream);
        DownSamplingLevels.InitializeLevels(config);
        pVDataTable = TableName.valueOf(config.pvTableName());
        pVDownSamplingDataTable = TableName.valueOf(config.downsampledPVTableName());
        pVDownSamplingJobTable = TableName.valueOf(config.jobTableName());
        inited = false;
//        System.out.println(pVDataTable);
//        System.out.println(pVDownSamplingDataTable);
//        System.out.println(pVDataTable);
        Configuration conf = null;
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", config.zookeeperQuorum());
        conf.set("hbase.zookeeper.property.clientPort", Integer.toString(config.zookeeperClientPort()));
        try {
            logger.info("connect to :" + config.zookeeperQuorum() + ":" + config.zookeeperClientPort());
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            logger.error("Exception in create connect to HBase" + e.getMessage());
            return false;
        }
        logger.info("connection established.");

        pVIdmanager.initialize(conn, config);
        statPlugin = new StatInformationPlugin(conn, config.pvTableName(), config.maxSecondsForRawSampleRow());
        byte[] columnFamily = new byte[]{'p'};
        List<byte[]> cfList = new ArrayList<>();
        cfList.add(columnFamily);
        HBaseTableManager tableMgr = new HBaseTableManager(conn);

        tableMgr.createTable(pVDataTable, cfList,
                createRegionKey(config.regionSplitBits4PVTable(), CommonConstants.PV_TABLE_ROWKEY_LEN));
        tableMgr.createTable(pVDownSamplingDataTable, cfList, createRegionKey(config.regionSplitBits4DownSampledTable(),
                CommonConstants.DOWN_SAMPLE_TABLE_ROW_KEY_LEN));
        tableMgr.createTable(pVDownSamplingJobTable, cfList, null);
        inited = true;
        return true;
    }

    private byte[][] createRegionKey(int bitLength, int keylength) {
        return IntStream.range(0, (int) Math.pow(2, bitLength)).boxed().map(a -> {
            byte[] c = new byte[keylength];
            a = a << (32 - bitLength);
            byte[] r = transformLongTo4Bytes(a);
            System.arraycopy(r, 0, c, 0, 4);
            return c;
        }).collect(Collectors.toList()).toArray(new byte[0][9]);
    }

    /**
     * Append samples in 1024 seconds to HBASE. Create down sampling samples and insert them to the HBase at the same
     * time.
     */
    @Override
    public boolean appendData(String pvName, List<Event> eventList, ArchDBRTypes eventType, String meta)
            throws IOException {
        if (!inited) {
            logger.error("the connection is not initialized");
            return false;
        }

        if (eventList == null) {
            logger.error("the event list is null!");
            return false;
        }
        if (eventList.isEmpty()) {
            logger.debug("the event list is empty! Nothing will insert to HBase.");
            return true;
        }

        Event firstEvent = null;
        firstEvent = getFirstEvent(eventList);

        if (firstEvent == null) {
            logger.error("No sample in the EventStream!");
            return false;
        }

        List<DownSamplingLevels> createDownsamplingLevels = new ArrayList<>();
        for (int i = 0; i < config.rawDataCreateDownSampleLevelNumber(); i++) {
            createDownsamplingLevels.add(DownSamplingLevels.getLevel((byte) i));
        }

        Set<DownSamplingMethods> downSapmlingMethods = new HashSet<>();
        if (supportCalculatePVTypes.contains(firstEvent.getDBRType())) {
            downSapmlingMethods = EnumSet.range(DownSamplingMethods.FIRST, DownSamplingMethods.MIN);
        } else {
            downSapmlingMethods.add(DownSamplingMethods.FIRST);
        }

        // Check the PV existed or not
        PvInfo info = pVIdmanager.getPVInfo(pvName);
        if (info == null) {

            // If does not exist create for it.
            info = pVIdmanager.createPVInfo(pvName, firstEvent.getDBRType(), meta);
            if (info == null) {
                logger.error("Create PV information for PV:" + pvName + " error!");
                return false;
            }
        }
        // If this PV has be deleted, recover it:
        if (info.getDeletedTime() != 0) {
            pVIdmanager.reCoverDeltedPv(info);
        }

        byte[] pVId = info.getPvId();
        Map<DownSamplingLevels, EnumMap<DownSamplingMethods, List<Event>>> downSampletataForCreatePB = DownSampleUtility
                .createDownSampledDateForStream(eventList, pVId, config.rawDataCreateDownSampleLevelNumber(),
                        info.getPvType(), createDownsamplingLevels, downSapmlingMethods);
        // set the raw data rows:
        RowsForEventPBData orignialRowDataForAppend = new RowsForEventPBData(pvName, pVId, eventList,
                config.maxRowSize(), config.maxSecondsForRawSampleRow());

        // set down sampling data rows:
        Map<DownSamplingLevels, EnumMap<DownSamplingMethods, RowsForEventPBData>> downSampleDataForAppend = createDownsampledPBForEachLevel(
                createDownsamplingLevels, downSapmlingMethods, pvName, pVId, downSampletataForCreatePB);

        // create PB and send them.
        insertRawDataToPVTableByPut(orignialRowDataForAppend);
        insertDataToDownSamplingTableByPut(downSampleDataForAppend, createDownsamplingLevels, downSapmlingMethods);
        return true;
    }

    public boolean updatePV(PvInfo info) throws IOException {
        if (!inited) {
            logger.error("the connection is not initialized");
            return true;
        }

        if (info == null) {
            logger.error("the update info is null!");
            return true;
        }

        // Check the PV existed or not
        PvInfo oldInfo = pVIdmanager.getPVInfo(info.getPvName());
        if (oldInfo == null) {
            logger.error("Can't found PV" + info.getPvName() + " in the HBase.");
            return true;
        }

        pVIdmanager.updatePV(info);
        return true;
    }

    public Map<String, PvInfo> getAllPVInfo() throws IOException {
        if (!inited) {
            logger.error("get PV Infor list error: the connection is not initialized");
            return null;
        }
        return pVIdmanager.getPVInfoList();
    }

    private Map<DownSamplingLevels, EnumMap<DownSamplingMethods, RowsForEventPBData>> createDownsampledPBForEachLevel(
            List<DownSamplingLevels> createDownsamplingLevels, Set<DownSamplingMethods> downSapmlingMethods,
            String pvName, byte[] pVId,
            Map<DownSamplingLevels, EnumMap<DownSamplingMethods, List<Event>>> downSampletDataForCreatePB) {
        Map<DownSamplingLevels, EnumMap<DownSamplingMethods, RowsForEventPBData>> downSampleDataForAppend = new HashMap<>();
        for (DownSamplingLevels lv : createDownsamplingLevels) {
            EnumMap<DownSamplingMethods, RowsForEventPBData> samplesDataInLevel = new EnumMap<>(
                    DownSamplingMethods.class);
            for (DownSamplingMethods m : downSapmlingMethods) {
                if (!downSampletDataForCreatePB.get(lv).get(m).isEmpty()) {
                    List<Event> el = downSampletDataForCreatePB.get(lv).get(m);
                    RowsForEventPBData rows = new RowsForEventPBData(pvName, pVId, el, config.maxRowSize(),
                            lv.getmaxPeriodInOneRow());
                    samplesDataInLevel.put(m, rows);
                }
            }
            downSampleDataForAppend.put(lv, samplesDataInLevel);
        }
        return downSampleDataForAppend;
    }

    private void insertDataToDownSamplingTableByPut(
            Map<DownSamplingLevels, EnumMap<DownSamplingMethods, RowsForEventPBData>> dtataForAppend,
            List<DownSamplingLevels> supportedDownsamplingLevels, Set<DownSamplingMethods> supporteddownSapmlingMethods)
            throws IOException {
        Table table = conn.getTable(pVDownSamplingDataTable);
        List<Put> puts = new ArrayList<>();
        for (DownSamplingLevels lv : supportedDownsamplingLevels) {
            for (DownSamplingMethods m : supporteddownSapmlingMethods) {
                if (dtataForAppend.containsKey(lv) && dtataForAppend.get(lv).containsKey(m)) {
                    RowsForEventPBData rowDatas = dtataForAppend.get(lv).get(m);
                    List<rowData> rows = dtataForAppend.get(lv).get(m).getPBRowsOfSamples();
                    for (rowData row : rows) {
                        byte[] rowKey = generateDownSamplingRowKey((int) row.getTimestamp(), rowDatas.getpVId(), lv, m);
                        Put putRow = new Put(rowKey);
                        putRow.addColumn(Bytes.toBytes("p"), Bytes.toBytes("d"), row.getPb());
                        puts.add(putRow);
                        if (puts.size() >= config.maxNumberOfPutRows()) {
                            table.put(puts);
                            puts.clear();
                        }
                    }
                }
            }
        }

        if (!puts.isEmpty()) {
            table.put(puts);
        }
        table.close();
    }

    private void insertRawDataToPVTableByPut(RowsForEventPBData dtataForAppend) throws IOException {
        List<rowData> rows = dtataForAppend.getPBRowsOfSamples();
        // Append raw data to PV Tabel in HBase:
        Table table = conn.getTable(pVDataTable);
        List<Put> puts = new ArrayList<Put>();
        for (rowData row : rows) {
            /*
             * Create row key for raw row
             */
            byte[] rowKeyForOriginalRow = generateRowKeyForRawEvent((int) row.getTimestamp(), dtataForAppend.getpVId());
            Put put = new Put(rowKeyForOriginalRow);
            put.addColumn(Bytes.toBytes("p"), Bytes.toBytes("d"), row.getPb()); // add column
            puts.add(put);
            if (puts.size() >= config.maxNumberOfPutRows()) {
                table.put(puts);
                puts.clear();
            }
        }
        if (!puts.isEmpty()) {
            table.put(puts);
        }
        table.close();
    }

    @Override
    public boolean appendDataList(String pvName, List<List<Event>> eventsList, ArchDBRTypes eventType, String metaData)
            throws IOException {
        if (!inited) {
            logger.error("the connection is not initialized");
            return false;
        }

        if (eventsList == null || eventsList.get(0) == null) {
            logger.error("the event list is null!");
            return false;
        }

        if (eventsList.isEmpty() || eventsList.get(0).isEmpty()) {
            logger.debug("the event list is empty! Nothing will insert to HBase.");
            return true;
        }

        List<DownSamplingLevels> createDownsamplingLevels = new ArrayList<>();
        for (int i = 0; i < config.rawDataCreateDownSampleLevelNumber(); i++) {
            createDownsamplingLevels.add(DownSamplingLevels.getLevel((byte) i));
        }

        Set<DownSamplingMethods> downSapmlingMethods = new HashSet<>();
        if (supportCalculatePVTypes.contains(eventsList.get(0).get(0).getDBRType())) {
            downSapmlingMethods = EnumSet.range(DownSamplingMethods.FIRST, DownSamplingMethods.MIN);
        } else {
            downSapmlingMethods.add(DownSamplingMethods.FIRST);
        }

        for (List<Event> stream : eventsList) {

            Event firstEvent = null;
            firstEvent = getFirstEvent(stream);
            if (firstEvent == null) {
                logger.error("No sample in the EventStream!");
                return false;
            }

            // Check the PV existed or not
            PvInfo info = pVIdmanager.getPVInfo(pvName);
            if (info == null) {
                // If does not exist create for it.
                info = pVIdmanager.createPVInfo(pvName, firstEvent.getDBRType(), metaData);
                if (info == null) {
                    logger.error("Create PV information for PV:" + pvName + " error!");
                    return false;
                }
            }

            // If this PV has be deleted, recover it:
            if (info.getDeletedTime() != 0) {
                pVIdmanager.reCoverDeltedPv(info);
            }

            byte[] pVId = info.getPvId();

            Map<DownSamplingLevels, EnumMap<DownSamplingMethods, List<Event>>> downSampletataForCreatePB = DownSampleUtility
                    .createDownSampledDateForStream(stream, pVId, config.rawDataCreateDownSampleLevelNumber(),
                            info.getPvType(), createDownsamplingLevels, downSapmlingMethods);

            RowsForEventPBData orignialRowDataForAppend = new RowsForEventPBData(pvName, pVId, stream,
                    config.maxRowSize(), config.maxSecondsForRawSampleRow());
            Map<DownSamplingLevels, EnumMap<DownSamplingMethods, RowsForEventPBData>> downSampleDataForAppend = createDownsampledPBForEachLevel(
                    createDownsamplingLevels, downSapmlingMethods, pvName, pVId, downSampletataForCreatePB);
            insertRawDataToPVTableByPut(orignialRowDataForAppend);
            insertDataToDownSamplingTableByPut(downSampleDataForAppend, createDownsamplingLevels, downSapmlingMethods);
        }
        return true;
    }

    @Override
    public List<Event> getData(String pvName, Timestamp startTime, Timestamp endTime,
                               PostProcessing downSamplingIdentify, int intervalSeconds, boolean enableCache, double samplePeriod)
            throws IOException {

        if (!inited) {
            logger.error("Not initialized. Please invoke the initialize before calling getData function.");
            return new ArrayList<>();
        }

        DownSamplingMethods method = PostProcessing2DownSamplingMethods.get(downSamplingIdentify);
        /**
         * Check the PV existed or not
         */
        PvInfo info = pVIdmanager.getPVInfo(pvName);
        if (info == null) {
            logger.info("No such PV for query data. PV name:" + pvName);
            return new ArrayList<>();
        }

        if (info.getDeletedTime() != 0) {
            logger.info("Can't query this PV. It has be marked as delted. PV name:" + pvName);
            return new ArrayList<>();
        }

        int startTimeStamp = (int) (startTime.getTime() / 1000);
        int endTimeStamp = (int) (endTime.getTime() / 1000);
        int timeSpan = endTimeStamp - startTimeStamp;
        if (timeSpan < 0) {
            logger.debug("the end time is litter than start time");
            return new ArrayList<>();
        }


        DownSamplingLevels level = calculateLevel(method, timeSpan, samplePeriod);
        boolean isUsingDownSamplingTable = (level != null);
        /*
         * Adjust the start time. considering this case: The event in the given start time is not the first one of a
         * row. So the time in row key will small than the given start time. If use the given start time as a row key
         * time to query, the return events will only start with the event in the next row. So we should adjust the
         * start time.
         */
        int startTimeForQueryRowKey = startTimeStamp - config.maxSecondsForRawSampleRow();
        byte[] startRowKey = generateRowKeyForRawEvent(startTimeForQueryRowKey, info.getPvId());
        byte[] endRowKey = generateRowKeyForRawEvent(endTimeStamp, info.getPvId());

        // if is not query the raw data, we create the row key for down sampling table:
        if (isUsingDownSamplingTable) {
            startTimeForQueryRowKey = startTimeStamp - level.getmaxPeriodInOneRow();
            startRowKey = generateDownSamplingRowKey(startTimeForQueryRowKey, info.getPvId(), level, method);
            endRowKey = generateDownSamplingRowKey(endTimeStamp, info.getPvId(), level, method);
        }

        List<List<Event>> pvEventsList = scanDataFromHBase(info, startRowKey, endRowKey, isUsingDownSamplingTable,
                startTimeStamp, enableCache, method == DownSamplingMethods.AVERAGE);
        if (pvEventsList.isEmpty()) {
            logger.debug("can not retrieval data from HBase for PV: " + pvName + " time:" + startTime);
            return new ArrayList<>();
        }
        // The last row maybe contain some data newer than the endTimeStamp.
        pvEventsList.set(pvEventsList.size() - 1, pvEventsList.get(pvEventsList.size() - 1).stream()
                .filter(e -> e.getEpochSeconds() <= endTimeStamp).collect(Collectors.toList()));
        List<Event> result = pvEventsList.stream().flatMap(List::stream).collect(Collectors.toList());
        logger.debug("Retrieval " + pvEventsList.size() + " rows, and " + result.size() + " events from Hbase for "
                + pvName);
        return result;
    }

    @Override
    public void renamePV(String oldName, String newName) throws IOException {
        logger.info("Rename " + oldName + " PV to: " + newName);
        PvInfo info = pVIdmanager.getPVInfo(oldName);
        if (info == null) {
            logger.info("can not found the PV: " + oldName + " to rename.");
        }
        pVIdmanager.reNamePv(info, newName);
    }

    @Override
    public void deleteData(String pvName) throws IOException {
        logger.info("Delete data in Hadoop for PV name " + pvName);
        PvInfo info = pVIdmanager.getPVInfo(pvName);
        if (info != null) {
            pVIdmanager.deltedPv(info);
            return;
        }
        logger.info("No such PV in the PV Table. The PV name is:" + pvName);
    }

    @Override
    public Event getFirstKnownEvent(String pvName) throws IOException {
        return null;
    }

    @Override
    public Event getLastKnownEvent(String pvName) throws IOException {
        return null;
    }

    @Override
    public Integer getSamplingInterval(PostProcessing downSamplingType, Timestamp startTime, Timestamp endTime,
                                       double samplePeriod) {
        DownSamplingMethods method = PostProcessing2DownSamplingMethods.get(downSamplingType);
        if (method == DownSamplingMethods.RAW) {
            return 0;
        }
        int startTimeStamp = (int) (startTime.getTime() / 1000);
        int endTimeStamp = (int) (endTime.getTime() / 1000);
        int timeSpan = endTimeStamp - startTimeStamp;
        if (timeSpan < 0) {
            logger.debug("the end time is litter than start time");
            return null;
        }
        DownSamplingLevels level = calculateLevel(method, timeSpan, samplePeriod);
        if (level == null) {
            return 0;
        }
        return level.getSamplingSeconds();
    }

    @Override
    public String getMeta(String pvName) throws IOException {
        PvInfo info = pVIdmanager.getPVInfo(pvName);
        if (info == null) {
            logger.info("No such PV for query data. PV name:" + pvName);
            return null;
        }
        return info.getMetaOfPV();
    }

    @Override
    public long getTotalSpace() throws IOException {
        return Long.MAX_VALUE;
    }

    @Override
    public long getUsableSpace() throws IOException {
        return getTotalSpace() - spaceConsumedByPV(null);
    }

    @Override
    public long spaceConsumedByPV(String pvName) throws IOException {
        return 0;
    }

    private Event getFirstEvent(List<Event> stream) {
        if (stream.size() <= 0) {
            logger.error("the Event list is empty!");
            return null;
        }
        return stream.get(0);
    }

    /**
     * Generate a row key for PV samples. RowKey is composed by: PV ID(5 bytes) + Sampling Type(high 4 bits) + Down
     * Sampling Level (low 4 bits)+ PV Sampled Timestamp(4 bytes)
     *
     * @param timeStamp          - the time stamp of this row.
     * @param pVId               - PV ID of this PV
     * @param level              - Down sampling level.
     * @param downSamplingMethod - down sampling method.
     * @throws IOException
     */
    private byte[] generateDownSamplingRowKey(int timeStamp, byte[] pVId, DownSamplingLevels level,
                                              DownSamplingMethods downSamplingMethod) throws IOException {
        byte[] typeLevel = {(byte) (downSamplingMethod.getValueInRowKey() | level.getValueInRowKey())};
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(buffer);
        output.write(pVId);
        output.write(typeLevel);
        output.writeInt(timeStamp);
        return buffer.toByteArray();
    }

    /**
     * Generate a row key for PV raw data samples. RowKey is composed by: PV ID(4 bytes) + PV Sampled Timestamp(4 bytes)
     *
     * @param timeStamp - the time stamp of this row.
     * @param pVId      - PV ID of this PV
     * @throws IOException if access HBase failed.
     */
    private byte[] generateRowKeyForRawEvent(int timeStamp, byte[] pVId) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(buffer);
        output.write(pVId);
        output.writeInt(timeStamp);
        return buffer.toByteArray();
    }

    /**
     * Scan data from HBase.
     *
     * @param info                   - PV infomation.
     * @param startRowKey            - the start row key for scan.
     * @param endRowKey              - the end row key for scan.
     * @param usingDownSamplingTable - if true than use the down sample table as the scan table, otherwise use the PV table.
     * @param enableCache            - use the cache buffer on HBase server or not.
     * @param isAverage              - set it to true if scan the average data, otherwise set it to false.
     * @return the scan result.
     * @throws IOException if access data base failed.
     */
    private List<List<Event>> scanDataFromHBase(PvInfo info, byte[] startRowKey, byte[] endRowKey,
                                                boolean usingDownSamplingTable, long startTimeStamp, boolean enableCache, boolean isAverage) throws IOException {
            Table table = null;
            if (!usingDownSamplingTable) {
                table = conn.getTable(pVDataTable);
            } else {
                table = conn.getTable(pVDownSamplingDataTable);
            }

            Scan scan = new Scan();
            scan.withStartRow(startRowKey, true);
            scan.withStopRow(endRowKey, true);
            scan.setCaching(config.cacheRowsWhenScan());
            scan.setCacheBlocks(enableCache);
            //scan.setMaxResultSize(config.scanMaxResultSize());
            List<List<Event>> pvEventsList = new ArrayList<>();
            ResultScanner scanner = table.getScanner(scan);
            boolean startPositionDetected = false;

            try {
                for (Result rs : scanner) {
                    ByteBuffer bb;
                    if (usingDownSamplingTable) {
                        // Get the time stamp in the down sampled PV table.
                        bb = ByteBuffer.wrap(rs.getRow(), CommonConstants.PVID_SIZE + CommonConstants.TYPE_LEVEL_SIZE,
                                CommonConstants.PV_TIME_STAMP_LEN);
                    } else {
                        // Get the time stamp in the PV table.
                        bb = ByteBuffer.wrap(rs.getRow(), CommonConstants.PVID_SIZE, CommonConstants.PV_TIME_STAMP_LEN);
                    }

                    long timeStampOfRow = ((long) bb.asIntBuffer().get()) * 1000;
                    DateTime dt = new DateTime(timeStampOfRow, DateTimeZone.UTC);
                    Cell[] cells = rs.rawCells();

                    for (Cell cell : cells) {
                        List<Event> decodedEvent = new ArrayList<>();
                        byte qualifier = cell.getQualifierArray()[cell.getQualifierOffset()];
                        if (qualifier == 'd') {
                            byte[] pbData = new byte[cell.getValueLength()];
                            System.arraycopy(cell.getValueArray(), cell.getValueOffset(), pbData, 0, cell.getValueLength());

                            if (!isAverage) {
                                decodedEvent = PB2Event.PBList2EventListConvert(info.getPvType(), (short) dt.getYear(),
                                        pbData);
                            } else {
                                decodedEvent = PBMeanScalarDoubleEvent
                                        .pBArray2MinIntEventListConvert((short) dt.getYear(), pbData).stream()
                                        .map(a -> a.toScalarEvent(info.getPvType())).collect(Collectors.toList());
                            }

                            if (!startPositionDetected && !decodedEvent.isEmpty()
                                    && (decodedEvent.get(decodedEvent.size() - 1).getEpochSeconds() >= startTimeStamp)) {
                                // filter the events not in the region of query.
                                startPositionDetected = true;
                                decodedEvent = decodedEvent.stream().filter(a -> a.getEpochSeconds() >= startTimeStamp)
                                        .collect(Collectors.toList());
                            }
                            if (startPositionDetected && !decodedEvent.isEmpty()) {
                                pvEventsList.add(decodedEvent);
                            }
                            break;
                        }
                    }
                }

            } catch (Exception ex) {

                logger.error("Scan data error! exception:" + ex.getMessage());
                throw new IOException(ex.getMessage());
            }

            return pvEventsList;
    }

    /**
     * according to the time span, to calculate which level can return the number of event near the
     * maxReturnEventsForQuery.
     *
     * @param method   - the down sampling method. null for raw data.
     * @param timeSpan - the time span in second unit for scan.
     * @return the level. null for raw.
     */
    private DownSamplingLevels calculateLevel(DownSamplingMethods method, int timeSpan, double samplePeriod) {
        // Calculate the level:
        DownSamplingLevels level = null;
        if ((timeSpan / samplePeriod) < config.maxReturnEventsForQuery()) {
            return level;
        }

        if (method == DownSamplingMethods.RAW) {
            return null;
        } else {
            for (DownSamplingLevels lv : DownSamplingLevels.getLevels()) {
                level = lv;
                if (lv.getmaxPeriodInOneRow() > timeSpan) {
                    break;
                }
            }
        }
        return level;
    }

    @Override
    public StatInformation getStat(String pvName, Timestamp startTime, Timestamp endTime) throws IOException {
        PvInfo info = pVIdmanager.getPVInfo(pvName);
        if (info == null) {
            logger.info("Can not found the PV " + pvName);
            return null;
        }
        return statPlugin.getStat(info.getPvId(), startTime, endTime, info.getPvType().getIntegerMap());
    }

    /**
     * convert a long to 5 bytes array.
     *
     * @param value - a long
     * @return 4 bytes array
     */
    public static byte[] transformLongTo4Bytes(long value) {
        return new byte[]{(byte) ((value >> 24) & 0xff), (byte) ((value >> 16) & 0xff), (byte) ((value >> 8) & 0xff),
                (byte) (value & 0xff),};
    }

}
