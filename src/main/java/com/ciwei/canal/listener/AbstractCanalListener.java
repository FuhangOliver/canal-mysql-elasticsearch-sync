package com.ciwei.canal.listener;

import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.ciwei.canal.event.AbstractCanalEvent;
import com.ciwei.canal.model.DatabaseTableModel;
import com.ciwei.canal.model.IndexTypeModel;
import com.ciwei.canal.service.MappingService;
import com.ciwei.canal.service.MultiMappingService;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author fuhang
 * @description: 监听器，监听canal客户端
 * @date 2020/5/26 11:26
 */
public abstract class AbstractCanalListener<EVENT extends AbstractCanalEvent> implements ApplicationListener<EVENT> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractCanalListener.class);

    @Resource
    private MappingService mappingService;

    @Resource
    private MultiMappingService multiMappingService;

    @Override
    public void onApplicationEvent(EVENT event) {
        Entry entry = event.getEntry();
        String database = entry.getHeader().getSchemaName();
        String table = entry.getHeader().getTableName();
        IndexTypeModel indexTypeModel = mappingService.getIndexType(new DatabaseTableModel(database, table));
        List<IndexTypeModel> indexTypeModelList = multiMappingService.getIndexType(new DatabaseTableModel(database, table));
        if (indexTypeModel == null && indexTypeModelList == null) {
            return;
        }
        RowChange change;
        try {
            change = RowChange.parseFrom(entry.getStoreValue());
        } catch (InvalidProtocolBufferException e) {
            logger.error("canalEntry_parser_error,根据CanalEntry获取RowChange失败！", e);
            return;
        }
        Optional.ofNullable(indexTypeModel).ifPresent(indexTypeModelNotNull -> {
            String index = indexTypeModelNotNull.getIndex();
            change.getRowDatasList().forEach(rowData -> doSync(database, table, index, rowData));
        });
        Optional.ofNullable(indexTypeModelList).ifPresent(indexTypeModelListNotNull -> {
            List<String> indexList = indexTypeModelListNotNull.stream().map(IndexTypeModel::getIndex).collect(Collectors.toList());
            change.getRowDatasList().forEach(rowData -> doMultiSync(database, table, indexList, rowData));
        });
    }

    Map<String, Object> parseColumnsToMap(List<Column> columns) {
        Map<String, Object> jsonMap = new HashMap<>();
        columns.forEach(column -> {
            if (column == null) {
                return;
            }
            jsonMap.put(column.getName(), column.getIsNull() ? null : mappingService.getElasticsearchTypeObject(column.getMysqlType(), column.getValue()));
        });
        return jsonMap;
    }

    /**
     * mysql 单表对应单个索引的数据同步的方法
     *
     * @param database 所属的数据库
     * @param table    所属的表
     * @param index    所属的索引
     * @param rowData  数据行
     */
    protected abstract void doSync(String database, String table, String index, RowData rowData);

    /**
     * mysql 多表对应单个索引的数据同步的方法，多表对应一个索引所以，一个表可能会属于多个es索引
     *
     * @param database  所属的数据库
     * @param table     所属的表
     * @param indexList 所属的索引的列表
     * @param rowData   数据行
     */
    protected abstract void doMultiSync(String database, String table, List<String> indexList, RowData rowData);
}
