package com.ciwei.canal.listener;

import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.ciwei.canal.event.InsertAbstractCanalEvent;
import com.ciwei.canal.service.ElasticsearchService;
import com.ciwei.canal.service.MappingService;
import com.ciwei.canal.service.MultiMappingService;
import com.ciwei.canal.util.JsonUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author fuhang
 * @description: 插入事件监听器
 * @date 2020/5/26 11:26
 */
@Component
public class InsertCanalListener extends AbstractCanalListener<InsertAbstractCanalEvent> {
    private static final Logger logger = LoggerFactory.getLogger(InsertCanalListener.class);

    @Resource
    private MappingService mappingService;

    @Resource
    private MultiMappingService multiMappingService;

    @Resource
    private ElasticsearchService elasticsearchService;

    @Override
    protected void doSync(String database, String table, String index, RowData rowData) {
        List<Column> columns = rowData.getAfterColumnsList();
        String primaryKey = Optional.ofNullable(mappingService.getTablePrimaryKeyMap().get(database + "." + table)).orElse("id");
        Column idColumn = columns.stream().filter(column -> column.getIsKey() && primaryKey.equals(column.getName())).findFirst().orElse(null);
        if (idColumn == null || StringUtils.isBlank(idColumn.getValue())) {
            logger.warn("insert_column_find_null_warn insert从column中找不到主键,database=" + database + ",table=" + table);
            return;
        }
        logger.debug("insert_column_id_info insert主键id,database=" + database + ",table=" + table + ",id=" + idColumn.getValue());
        Map<String, Object> dataMap = parseColumnsToMap(columns);
        elasticsearchService.insertById(index, idColumn.getValue(), dataMap);
        logger.debug("insert_es_info 同步es插入操作成功！database=" + database + ",table=" + table + ",data=" + JsonUtil.toJson(dataMap));
    }

    @Override
    protected void doMultiSync(String database, String table, List<String> indexList, RowData rowData) {
        indexList.stream().forEach(index -> {
            List<Column> columns = rowData.getAfterColumnsList();
            String primaryKey = Optional.ofNullable(multiMappingService.getMultiTablePrimaryKey(database,table,index)).orElse("id");
            Column idColumn = columns.stream().filter(column -> primaryKey.equals(column.getName())).findFirst().orElse(null);
            if (idColumn == null || StringUtils.isBlank(idColumn.getValue())) {
                logger.warn("insert_column_find_null_warn insert从column中找不到主键,database=" + database + ",table=" + table);
                return;
            }
            logger.debug("insert_column_id_info insert主键id,database=" + database + ",table=" + table + ",id=" + idColumn.getValue());
            Map<String, Object> dataMap = parseColumnsToMap(columns);
            this.elasticsearchService.syncMysqlToEsSuper(index, idColumn.getValue(), table, dataMap);
            logger.debug("insert_es_info 同步es插入操作成功！database=" + database + ",table=" + table + ",data=" + JsonUtil.toJson(dataMap));
        });
    }
}
