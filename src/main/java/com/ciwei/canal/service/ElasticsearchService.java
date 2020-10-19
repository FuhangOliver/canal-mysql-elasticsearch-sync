package com.ciwei.canal.service;

import javafx.util.Pair;

import java.util.List;
import java.util.Map;

/**
 * @author fuhang
 * @description: 操作es数据库
 * @date 2020/5/26 11:26
 */
public interface ElasticsearchService {
    /**
     * es的id作为主键_id，插入单条数据
     *
     * @param index   索引
     * @param id      主键
     * @param dataMap 数据
     */
    void insertById(String index, String id, Map<String, Object> dataMap);

    /**
     * 批量插入多条数据
     *
     * @param index     索引
     * @param idDataMap 包含id的数据
     */
    void batchInsertById(String index, Map<String, Map<String, Object>> idDataMap);

    /**
     * 根据_id更新单条数据
     *
     * @param index   索引
     * @param id      主键
     * @param dataMap 数据
     */
    void update(String index, String id, Map<String, Object> dataMap);

    /**
     * 根据_id删除单条数据
     *
     * @param index 索引
     * @param id    主键
     */
    void deleteById(String index, String id);

    /**
     * 通过文档id判断文档是否存在
     *
     * @param index 索引
     * @param id    主键
     * @return 是否存在
     */
    boolean isExist(String index, String id);

    /**
     * 同步Mysql数据到es
     *
     * @param index     索引
     * @param id        主键
     * @param tableName 更新的表名称
     * @param dataMap   数据
     */
    void syncMysqlToEsSuper(String index, String id, String tableName, Map<String, Object> dataMap);

    /**
     * 批量插入多条嵌套数据
     *
     * @param index     索引
     * @param tableName 更新的表名称
     * @param idDataMap 包含id的数据
     */
    void nestedBatchInsert(String index, String tableName, List<Pair<String, Map<String, Object>>> idDataMap);

    /**
     * 根据_id更新嵌套结构的数据
     *
     * @param index     索引
     * @param id        主键
     * @param tableName 更新的表名称
     * @param dataMap   数据
     */
    void nestedUpdate(String index, String id, String tableName, Map<String, Object> dataMap);

    /**
     * @param index      索引
     * @param queryField 使用哪个字段进行查询
     * @param tableName  更新的表名称
     * @param dataMap    数据
     */
    void nestedUpdateByQuery(String index, String queryField, String tableName, Map<String, Object> dataMap);

    /**
     * 根据_id插入嵌套结构的数据
     *
     * @param index     索引
     * @param id        主键
     * @param tableName 更新的表名称
     * @param dataMap   数据
     */
    void nestedInsert(String index, String id, String tableName, Map<String, Object> dataMap);

    /**
     * 根据_id新增嵌套结构的数据
     *
     * @param index     索引
     * @param id        主键
     * @param tableName 更新的表名称
     * @param dataMap   数据
     */
    void nestedAdd(String index, String id, String tableName, Map<String, Object> dataMap);

    /**
     * 根据_id删除嵌套结构的数据
     *
     * @param index     索引
     * @param id        主键
     * @param tableName 更新的表名称
     * @param dataMap   数据
     */
    void nestedDelete(String index, String id, String tableName, Map<String, Object> dataMap);

    /**
     * 判断索引下面nested的路径是否存在
     *
     * @param index     索引
     * @param id        主键
     * @param tableName 更新的表名称
     * @return 是否存在
     */
    boolean nestedPathExist(String index, String id, String tableName);

    /**
     * 根据索引名称判断索引是否存在
     *
     * @param index 索引
     * @return 是否存在
     */
    boolean checkIndexExists(String index);

    /**
     * 根据索引名称创建索引
     *
     * @param index 索引
     */
    void createIndex(String index);
}
