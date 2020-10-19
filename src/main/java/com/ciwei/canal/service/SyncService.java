package com.ciwei.canal.service;

import com.ciwei.canal.model.request.SyncByIndexRequest;
import com.ciwei.canal.model.request.SyncByTableRequest;

/**
 * @author fuhang
 * @description: 通过database和table同步数据库
 * @date 2020/5/26 11:26
 */
public interface SyncService {
    /**
     * 通过database和table同步数据库
     *
     * @param request 请求参数
     * @return 后台同步进程执行成功与否
     */
    boolean syncByTable(SyncByTableRequest request);

    /**
     * 通过ES的索引同步数据库数据到ES
     *
     * @param request 请求参数
     * @return 后台同步进程执行成功与否
     */
    boolean syncByElasticsearchIndex(SyncByIndexRequest request);
}
