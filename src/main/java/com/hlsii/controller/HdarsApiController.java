package com.hlsii.controller;

import com.alibaba.fastjson.JSONObject;
import com.hlsii.commdef.Constants;
import com.hlsii.commdef.DownloadTask;
import com.hlsii.commdef.PVDataFormat;
import com.hlsii.commdef.RetrieveParms;
import com.hlsii.metrics.IRetrievalMetrics;
import com.hlsii.service.*;
import com.hlsii.util.BeamCurrentUtil;
import com.hlsii.util.PVDataTreeUtil;
import com.hlsii.util.TimeUtil;
//import com.hlsii.util.WebUtil;
import com.hlsii.vo.RetrieveData;
import com.hlsii.vo.ReturnWrap;
import com.hlsii.vo.StatisticsData;
import hadarshbaseplugin.commdef.PostProcessing;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;


/**
 * @author ScXin
 * @date 4/26/2020 2:20 PM
 */

@RestController
@RequestMapping(value = "/hdars/api")
public class HdarsApiController {
    @Autowired
    private IArchiverSystemService aaService;
    @Autowired
    private IRetrieveService retrieveService;

    @Autowired
    private IDownloadService downloadService;

    @Autowired
    private IHealthService healthService;

    @Autowired
    private IRetrievalMetrics metricsService;



    private Logger logger = LoggerFactory.getLogger(getClass());


    //    @RequiresRoles(value={"admin","user","guest"},logical = Logical.OR )
    @ResponseBody
    @ApiOperation("根据PV名称，特征值类型，起止时间查询指定时间段的历史数据")
    @GetMapping("/history")
    public ReturnWrap getHistoryById(String pvListStr,
                                     String type,
                                     String from,
                                     String to) {
        if (pvListStr == null || pvListStr == "") {
            return new ReturnWrap("1", "please choose the pv", null);
        }

        int sampleDuration = 0;
        List<RetrieveData> result = new ArrayList<>();
        try {
            PostProcessing postProcessing = PostProcessing.FIRSTSAMPLE;
            if (!StringUtils.isEmpty(type)) {
                postProcessing = PostProcessing.parse(type);
            }
            if (postProcessing == PostProcessing.LASTSAMPLE || postProcessing == PostProcessing.NONE) {
                postProcessing = PostProcessing.FIRSTSAMPLE;
            }


            //将前端传过来的pv列表按照","进行分割，得到PV列表
//            List<String> pvList1 = Arrays.asList(pvListStr, ",");
            List<String> pvList1 = Arrays.asList(pvListStr.split(","));
            Timestamp startTime = TimeUtil.convertFromISO8601String(from);
            Timestamp endTime = TimeUtil.convertFromISO8601String(to);
            if (postProcessing != PostProcessing.NONE) {
                sampleDuration = retrieveService.getSamplingInterval(pvList1.get(0), postProcessing, startTime, endTime);
            }
            if (sampleDuration == 0) {
                postProcessing = PostProcessing.NONE;
            }

            RetrieveParms retrieveParms = new RetrieveParms(pvList1, postProcessing, sampleDuration, startTime, endTime, true, PVDataFormat.QW);
            result = retrieveService.retrievePVData(retrieveParms);
        } catch (Exception ex) {
            String msg = "Exception at retrieving data: " + ex.toString();
            logger.info(msg);
            return new ReturnWrap("1", msg);
        }
        ReturnWrap rc = new ReturnWrap();
        JSONObject data = new JSONObject();
        data.put("list", result);
        data.put("sampleDuration", sampleDuration);
        rc.setData(data);
        return rc;
    }

    //    @RequiresRoles(value={"admin","user"},logical = Logical.OR )\
    @ResponseBody
    @ApiOperation("根据PV名称和起始时间，查询这段时间的统计信息")
    @GetMapping("/history/statistics/{pv}/{startTime}/{endTime}")
    public ReturnWrap statistic(@PathVariable("pv") String pv,
                                @PathVariable("startTime") String from,
                                @PathVariable("endTime") String to) {
        ReturnWrap rw;
        Timestamp startTime = TimeUtil.convertFromISO8601String(from);
        Timestamp endTime = TimeUtil.convertFromISO8601String(to);
        try {
            StatisticsData statData = retrieveService.getPVStat(pv, startTime, endTime);
            JSONObject data = new JSONObject();
            data.put("pvName", pv);
            data.put("statistics", statData);
            rw = new ReturnWrap(true, data);
        } catch (IOException ex) {        //catch exception,and return to back-End
            String msg = "Exception at statistic:" + ex.toString();
            logger.info(msg);
            rw = new ReturnWrap("1", msg);
        }
        return rw;
    }

    //    @RequiresRoles(value={"admin","user"},logical = Logical.OR )
    @ApiOperation("创建下载任务，并计算下载信息")
    @GetMapping("/history/downloadInfo/{pvListStr}/{type}/{startTime}/{endTime}")
    public ReturnWrap downloadPVData(@PathVariable("pvListStr") String pvListStr,
                                     @PathVariable("type") String type,
                                     @PathVariable("startTime") String from,
                                     @PathVariable("endTime") String to) {


        PostProcessing postProcessing = PostProcessing.NONE;
        if (!StringUtils.isEmpty(type)) {
            postProcessing = PostProcessing.parse(type);
        }
        List<String> pvList = Arrays.asList(pvListStr.split(","));
        Timestamp startTime = TimeUtil.convertFromISO8601String(from);
        Timestamp endTime = TimeUtil.convertFromISO8601String(to);
        int sampleDuration = 0;
        if (postProcessing != PostProcessing.NONE) {
            sampleDuration = retrieveService.getSamplingInterval(pvList.get(0), postProcessing, startTime, endTime);
        }

        RetrieveParms retrieveParms = new RetrieveParms(pvList, postProcessing, sampleDuration,
                startTime, endTime, true, PVDataFormat.QW);
        BufferedRetrieveService in = new BufferedRetrieveService(retrieveService, retrieveParms);
        long length = in.getTotalSize();
        DownloadTask task = downloadService.createTask(retrieveParms);
        logger.info("Create download task:" + task.getId());
        JSONObject downloadInfo = DownloadController.estimateDownload(length);
        downloadInfo.put("taskid", task.getId());
        return new ReturnWrap(Constants.RETURN_SUCCESS, downloadInfo);
    }


    @ApiOperation("根据PV名称,起止时间,获取这段时间的数据量")
    @GetMapping("/history/page/count/{pvListStr}/{type}/{startTime}/{endTime}")
    public long getMaxCount(@PathVariable("pvListStr") String pvListStr,
                            @PathVariable("type") String type,
                            @PathVariable("startTime") String from,
                            @PathVariable("endTime") String to) {
        long count = 0;
        if (pvListStr == null || pvListStr == "") {
            return count;
        }

        int sampleDuration = 0;
        List<RetrieveData> result = new ArrayList<>();
        try {
            PostProcessing postProcessing = PostProcessing.FIRSTSAMPLE;
            if (!StringUtils.isEmpty(type)) {
                postProcessing = PostProcessing.parse(type);
            }
            if (postProcessing == PostProcessing.LASTSAMPLE || postProcessing == PostProcessing.NONE) {
                postProcessing = PostProcessing.FIRSTSAMPLE;
            }


            //将前端传过来的pv列表按照","进行分割，得到PV列表
//            List<String> pvList1 = Arrays.asList(pvListStr, ",");
            List<String> pvList1 = Arrays.asList(pvListStr.split(","));
            Timestamp startTime = TimeUtil.convertFromISO8601String(from);
            Timestamp endTime = TimeUtil.convertFromISO8601String(to);
            if (postProcessing != PostProcessing.NONE) {
                sampleDuration = retrieveService.getSamplingInterval(pvList1.get(0), postProcessing, startTime, endTime);
            }
            if (sampleDuration == 0) {
                postProcessing = PostProcessing.NONE;
            }

            RetrieveParms retrieveParms = new RetrieveParms(pvList1, postProcessing, sampleDuration, startTime, endTime, true, PVDataFormat.QW);
            result = retrieveService.retrievePVData(retrieveParms);
            for (RetrieveData retrieveData : result) {
                count = Math.max(retrieveData.getData().size(), count);
            }
        } catch (Exception ex) {
            String msg = "Exception at retrieving data: " + ex.toString();
            logger.info(msg);
            return 0;
        }
        return count;
    }

    @GetMapping("/history/page/{pageNum}/{pageSize}/{pvListStr}/{type}/{startTime}/{endTime}")
    public Map<String, List<Object>> getDateByPage(@PathVariable("pageNum") int pageNum,
                                                   @PathVariable("pageSize") int pageSize,
                                                   @PathVariable("pvListStr") String pvListStr,
                                                   @PathVariable("type") String type,
                                                   @PathVariable("startTime") String from,
                                                   @PathVariable("endTime") String to) {
        if (pvListStr == null || pvListStr == "") {
            return null;
        }

        int sampleDuration = 0;
        List<RetrieveData> result = new ArrayList<>();
        Map<String, List<Object>> pageDataMap = new HashMap<>();
        try {
            PostProcessing postProcessing = PostProcessing.FIRSTSAMPLE;
            if (!StringUtils.isEmpty(type)) {
                postProcessing = PostProcessing.parse(type);
            }
            if (postProcessing == PostProcessing.LASTSAMPLE || postProcessing == PostProcessing.NONE) {
                postProcessing = PostProcessing.FIRSTSAMPLE;
            }


            //将前端传过来的pv列表按照","进行分割，得到PV列表
//            List<String> pvList1 = Arrays.asList(pvListStr, ",");
            List<String> pvList1 = Arrays.asList(pvListStr.split(","));
            Timestamp startTime = TimeUtil.convertFromISO8601String(from);
            Timestamp endTime = TimeUtil.convertFromISO8601String(to);
            if (postProcessing != PostProcessing.NONE) {
                sampleDuration = retrieveService.getSamplingInterval(pvList1.get(0), postProcessing, startTime, endTime);
            }
            if (sampleDuration == 0) {
                postProcessing = PostProcessing.NONE;
            }

            RetrieveParms retrieveParms = new RetrieveParms(pvList1, postProcessing, sampleDuration, startTime, endTime, true, PVDataFormat.QW);
            result = retrieveService.retrievePVData(retrieveParms);


            for (RetrieveData retrieveData : result) {
                String pvName = retrieveData.getPvName();
                int size = retrieveData.getData().size();
                List<Object> dataList = retrieveData.getData().subList((pageNum - 1) * pageSize, Math.min(pageNum * pageSize, size));
                pageDataMap.put(pvName, dataList);
            }
        } catch (Exception ex) {
            String msg = "Exception at retrieving data: " + ex.toString();
            logger.info(msg);
            return null;
        }
        return pageDataMap;
    }

    //    @RequiresRoles(value={"admin","user"},logical = Logical.OR )
    @ApiOperation("获取分组树")
    @GetMapping("/history/grouptree")
    public ReturnWrap groupTree() {
        ReturnWrap result = new ReturnWrap();
        result.setData(PVDataTreeUtil.getPVDataTree());
        return result;
    }

    //    @RequiresRoles(value = {"admin", "user"}, logical = Logical.OR)
    @ApiOperation("根据组Id得到其下的PV")
    @GetMapping("/grouppv/{groupId}")
    public ReturnWrap grouppv(@PathVariable("groupId") String groupId) {
        ReturnWrap result = new ReturnWrap();
        result.setData(PVDataTreeUtil.getPVDataTree().getLeafGroup(groupId));
        return result;
    }

    //    @RequiresRoles(value={"admin","user","guest"},logical = Logical.OR )
    @GetMapping("/getMatchingPVs/{pv}/{limit}")
    public ReturnWrap getMatchingPVs(@PathVariable("pv") String pv,
                                     @PathVariable("limit") int limit) {
        ReturnWrap ret;
        try {
            String result = aaService.getMatchingPVs(limit, pv);
            List<String> dataResult = JSONObject.parseArray(result, String.class);
            ret = new ReturnWrap(true, dataResult);
        } catch (Exception e) {
            ret = new ReturnWrap(false, e.getMessage());
        }
        return ret;
    }

    //    @RequiresRoles(value={"admin","user","guest"},logical = Logical.OR )
    @ApiOperation("系统健康检查")
    @GetMapping("/healthcheck")
    public JSONObject checkHealth() {
        return healthService.getHealthStatus();
    }

    //    @RequiresRoles(value={"admin","user","guest"},logical = Logical.OR )
    @ApiOperation("获取BeamCurrent的配置信息")
    @GetMapping("/beamCurrentConfig")
    public ReturnWrap beamCurrentConfig() {
        ReturnWrap result = new ReturnWrap();
        result.setData(BeamCurrentUtil.getAll());
        return result;
    }

    //    @RequiresRoles(value={"admin"},logical = Logical.OR )
    @ApiOperation("获取系统Metrics相关信息")
    @GetMapping("/hadarsMetrics")
    public ReturnWrap hadarsMetrics() {
        ReturnWrap result = new ReturnWrap();
        result.setData(metricsService.getMetrics());
        return result;
    }

    //    @RequiresRoles(value={"admin","user"},logical = Logical.OR )
//    @ApiOperation("得到当前用户的所有的profile")
//    @ResponseBody
//    @GetMapping("/userprofiles")
//    public ReturnWrap usrProfiles() {
//        User user = webUtil.getCurrentLoginUser();
//        ReturnWrap rc = new ReturnWrap();
//        List<QueryProfile> profiles = queryProfileService.getUserProfiles(user.getId());
//        List<ProfileVO> profileVOs = new ArrayList<>();
//        for (QueryProfile profile : profiles) {
//            profileVOs.add(new ProfileVO(profile));
//        }
//        rc.setData(profileVOs);
//        return rc;
//    }

//    //    @RequiresRoles(value={"admin","user"},logical = Logical.OR )
//    @ApiOperation("根据profileID获取其group")
//    @ResponseBody
//    @GetMapping("/getProfile/{profId}")
//    public ReturnWrap getProfileGroups(@PathVariable("profId") String profId) {
//        QueryProfile profile = queryProfileService.get(profId);
//        if (profile == null) {
//            return new ReturnWrap(false, "Query profile not found!");
//        }
//        if (profile.getUser().getId().equals(webUtil.getCurrentLoginUser().getId())) {
//            return new ReturnWrap(true, GroupVO.fromGroups(queryProfileService.getProfileGroups(profId)));
//        }
//        return new ReturnWrap(false, "The query profile is not owned by you!");
//    }

//    @RequiresRoles(value = {"admin", "user"}, logical = Logical.OR)

    /**
     * 前端请求的格式为：queryParms += "&group=" + groupId + "," + groupName + "," + logarithm + ",(" + pvsStr + ")"
     *
     * @param profId
     * @param profName
     * @param profGroupList
     * @return
     */
//
//    @ApiOperation("保存profile的接口")
//    @ResponseBody
//    @GetMapping("/saveProfile/{profId}/{profName}/{profGroupList}")
//    public ReturnWrap saveProfileGroups(@PathVariable("profId") String profId,
//                                        @PathVariable("profName") String profName,
//                                        @PathVariable("profGroupList") String profGroupList) {
//        QueryProfile profile = new QueryProfile(profId, webUtil.getCurrentLoginUser(), profName);
//        String[] groupStr = profGroupList.split("&group");
//        Set<String> groupNameSet = new HashSet<>();
//        List<ProfileGroup> groups = new ArrayList<>();
//        if (groupStr != null) {
//            for (int i = 0; i < groupStr.length; i++) {
//                // Group format: id,name,logarithm,(pv1,pv2,pv3...)
//                String str = groupStr[i].trim();
//                if (org.apache.commons.lang3.StringUtils.isEmpty(str)) {
//                    continue;
//                }
//                String[] s = str.split(",\\(");
//                String[] groupInfo = s[0].split(",");
//                String pvsStr = s[1].substring(0, s[1].length() - 1);
//                boolean logarithm = Boolean.parseBoolean(groupInfo[2]);
//                ProfileGroup group = new ProfileGroup(groupInfo[0], profile, groupInfo[1], logarithm, pvsStr);
//                if (org.apache.commons.lang3.StringUtils.isEmpty(group.getGroupName())) {
//                    return new ReturnWrap(false, "Group name can't be empty.");
//                }
//                if (groupNameSet.contains(group.getGroupName())) {
//                    return new ReturnWrap(false, "Profile can't have duplicate group name.");
//                }
//                groupNameSet.add(group.getGroupName());
//                groups.add(group);
//            }
//        }
//        if (groups.isEmpty()) {
//            return new ReturnWrap(false, "No group is defined.");
//        }
////        QueryProfile profile1 = queryProfileService.saveProfile(profile, groups);
////        recordUserLogService.logOperation(OperationType.SAVE_PROFILE, profile1.getProfName());
////        return new ReturnWrap(true, new ProfileVO(profile1));
//    }

    //    @RequiresRoles(value={"admin","user"},logical = Logical.OR)
//    @ApiOperation("根据Id删除profile")
//    @ResponseBody
//    @DeleteMapping("/delProfile/{profId}")
//    public ReturnWrap delProfile(@PathVariable("profId") String profId) {
//        QueryProfile profile = queryProfileService.get(profId);
//        if (profile == null) {
//            return new ReturnWrap(false, "Query profile not found!");
//        }
//        if (profile.getUser().getId().equals(webUtil.getCurrentLoginUser().getId())) {
//            queryProfileService.deleteProfile(profile);
////            recordUserLogService.logOperation(OperationType.DELETE_PROFILE, profile.getProfName());
//            return new ReturnWrap();
//        }
//        return new ReturnWrap(false, "The query profile is not owned by you!");
//    }

}
