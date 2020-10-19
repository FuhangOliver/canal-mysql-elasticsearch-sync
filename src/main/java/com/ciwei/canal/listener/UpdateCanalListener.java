package com.ciwei.canal.listener;

import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.ciwei.canal.event.UpdateAbstractCanalEvent;
import com.ciwei.canal.service.ElasticsearchService;
import com.ciwei.canal.service.MappingService;
import com.ciwei.canal.service.MultiMappingService;
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
 * @description: 更新事件监听器
 * @date 2020/5/26 11:26
 */
@Component
public class UpdateCanalListener extends AbstractCanalListener<UpdateAbstractCanalEvent> {
    private static final Logger logger = LoggerFactory.getLogger(UpdateCanalListener.class);

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
            logger.warn("update_column_find_null_warn update从column中找不到主键,database=" + database + ",table=" + table);
            return;
        }
        logger.debug("update_column_id_info update主键id,database=" + database + ",table=" + table + ",id=" + idColumn.getValue());
        Map<String, Object> dataMap = parseColumnsToMap(columns);
        elasticsearchService.update(index, idColumn.getValue(), dataMap);
        logger.debug("update_es_info 同步es插入操作成功！database=" + database + ",table=" + table + ",data=" + dataMap);
    }

    @Override
    protected void doMultiSync(String database, String table, List<String> indexList, RowData rowData) {
        indexList.stream().forEach(index -> {
            List<Column> columns = rowData.getAfterColumnsList();
            String primaryKey = Optional.ofNullable(multiMappingService.getMultiTablePrimaryKey(database, table, index)).orElse("id");
            Column idColumn = columns.stream().filter(column -> primaryKey.equals(column.getName())).findFirst().orElse(null);
            if (idColumn == null || StringUtils.isBlank(idColumn.getValue())) {
                logger.warn("update_column_find_null_warn update从column中找不到主键,database=" + database + ",table=" + table);
                return;
            }
            logger.debug("update_column_id_info update主键id,database=" + database + ",table=" + table + ",id=" + idColumn.getValue());
            Map<String, Object> dataMap = parseColumnsToMap(columns);
            this.elasticsearchService.syncMysqlToEsSuper(index, idColumn.getValue(), table, dataMap);
            logger.debug("update_es_info 同步es插入操作成功！database=" + database + ",table=" + table + ",data=" + dataMap);
        });
    }
}
