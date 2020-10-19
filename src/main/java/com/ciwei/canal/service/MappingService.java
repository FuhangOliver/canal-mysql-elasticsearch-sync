package com.ciwei.canal.service;

import com.ciwei.canal.model.DatabaseTableModel;
import com.ciwei.canal.model.IndexTypeModel;

import java.util.Map;

/**
 * @author fuhang
 * @description: 配置文件映射服务
 * @date 2020/5/26 11:26
 */
public interface MappingService {

    /**
     * 通过database和table，获取与之对应的index
     *
     * @param databaseTableModel mysql
     * @return Elasticsearch
     */
    IndexTypeModel getIndexType(DatabaseTableModel databaseTableModel);

    /**
     * 通过index，获取与之对应的database和table
     *
     * @param indexTypeModel Elasticsearch
     * @return mysql
     */
    DatabaseTableModel getDatabaseTableModel(IndexTypeModel indexTypeModel);

    /**
     * 获取数据库表的主键映射
     *
     * @return 获取表对应es的组件
     */
    Map<String, String> getTablePrimaryKeyMap();

    /**
     * 设置数据库表的主键映射
     */
    void setTablePrimaryKeyMap(Map<String, String> tablePrimaryKeyMap);

    /**
     * 获取Elasticsearch的数据转换后类型
     *
     * @param mysqlType mysql数据类型
     * @param data      具体数据
     * @return Elasticsearch对应的数据类型
     */
    Object getElasticsearchTypeObject(String mysqlType, String data);
}
