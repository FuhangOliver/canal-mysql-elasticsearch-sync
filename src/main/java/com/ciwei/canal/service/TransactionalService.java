package com.ciwei.canal.service;


import com.ciwei.canal.model.DatabaseTableModel;
import com.ciwei.canal.model.IndexTypeModel;
import com.ciwei.canal.model.request.SyncByIndexRequest;
import com.ciwei.canal.model.request.SyncByTableRequest;

/**
 * @author fuhang
 * @description: 开启事务的读取mysql并插入到Elasticsearch中（读锁）
 * @date 2020/5/26 11:26
 */
public interface TransactionalService {

    /**
     * 通过mysql表来找索引，开启事务的读取mysql并插入到Elasticsearch中（读锁）
     *
     * @param request        通过mysql表同步，请求参数
     * @param primaryKey     es的主键
     * @param from           开始索引
     * @param to             结束索引
     * @param indexTypeModel es的索引
     */
    void batchInsertElasticsearch(SyncByTableRequest request, String primaryKey, long from, long to, IndexTypeModel indexTypeModel);

    /**
     * 通过es索引来找对应的mysql表，开启事务的读取mysql并插入到Elasticsearch中（读锁）
     *
     * @param request            通过es的索引同步，请求参数
     * @param primaryKey         es的主键
     * @param from               开始索引
     * @param to                 结束索引
     * @param databaseTableModel 对应的数据库表
     */
    void batchInsertElasticsearchByIndex(SyncByIndexRequest request, String primaryKey, long from, long to, DatabaseTableModel databaseTableModel);
}
