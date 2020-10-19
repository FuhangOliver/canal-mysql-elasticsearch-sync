package com.ciwei.canal.controller;

import com.ciwei.canal.model.request.SyncByIndexRequest;
import com.ciwei.canal.model.request.SyncByTableRequest;
import com.ciwei.canal.model.response.Response;
import com.ciwei.canal.service.SyncService;
import com.ciwei.canal.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;

/**
 * @author fuhang
 * @description: 通过库名和表名全量同步数据
 * @date 2020/5/26 11:26
 */
@Controller
@RequestMapping("/sync")
@Validated
public class SyncController {
    private static final Logger logger = LoggerFactory.getLogger(SyncController.class);

    @Resource
    private SyncService syncService;

    /**
     * 通过库名和表名全量同步数据
     *
     * @param request 请求参数
     */
    @RequestMapping("/byTable")
    @ResponseBody
    public String syncTable(@Validated SyncByTableRequest request) {
        logger.debug("request_info: " + JsonUtil.toJson(request));
        String response = Response.success(syncService.syncByTable(request)).toString();
        logger.debug("response_info: " + JsonUtil.toJson(request));
        return response;
    }

    /**
     * 通过索引名称全量同步数据
     *
     * @param request 请求参数
     */
    @RequestMapping("/byIndex")
    @ResponseBody
    public String syncIndex(@Validated SyncByIndexRequest request) {
        logger.debug("request_info: " + JsonUtil.toJson(request));
        String response = Response.success(syncService.syncByElasticsearchIndex(request)).toString();
        logger.debug("response_info: " + JsonUtil.toJson(request));
        return response;
    }
}
