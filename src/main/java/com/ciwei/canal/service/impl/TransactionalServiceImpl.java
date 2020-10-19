package com.ciwei.canal.service.impl;

import com.ciwei.canal.dao.BaseDao;
import com.ciwei.canal.model.DatabaseTableModel;
import com.ciwei.canal.model.IndexTypeModel;
import com.ciwei.canal.model.request.SyncByIndexRequest;
import com.ciwei.canal.model.request.SyncByTableRequest;
import com.ciwei.canal.service.ElasticsearchService;
import com.ciwei.canal.service.TransactionalService;
import javafx.util.Pair;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author fuhang
 * @description: 转换特殊字段的格式，批量更新es
 * @date 2020/5/26 11:26
 */
@Service
public class TransactionalServiceImpl implements TransactionalService {

    @Resource
    private BaseDao baseDao;

    @Resource
    private ElasticsearchService elasticsearchService;

    @Transactional(readOnly = true, rollbackFor = Exception.class)
    @Override
    public void batchInsertElasticsearch(SyncByTableRequest request, String primaryKey, long from, long to, IndexTypeModel indexTypeModel) {
        List<Map<String, Object>> dataList = baseDao.selectByPKIntervalLockInShareMode(primaryKey, from, to, request.getDatabase(), request.getTable());
        if (dataList == null || dataList.isEmpty()) {
            return;
        }
        dataList = convertDateType(dataList);
        Map<String, Map<String, Object>> dataMap = dataList.parallelStream().collect(Collectors.toMap(strObjMap -> String.valueOf(strObjMap.get(primaryKey)), map -> map));
        elasticsearchService.batchInsertById(indexTypeModel.getIndex(), dataMap);
    }

    @Transactional(readOnly = true, rollbackFor = Exception.class)
    @Override
    public void batchInsertElasticsearchByIndex(SyncByIndexRequest request, String primaryKey, long from, long to, DatabaseTableModel databaseTableModel) {
        List<Map<String, Object>> dataList = baseDao.selectByPKIntervalLockInShareMode(primaryKey, from, to, databaseTableModel.getDatabase(), databaseTableModel.getTable());
        if (dataList == null || dataList.isEmpty()) {
            return;
        }
        dataList = convertDateType(dataList);
        List<Pair<String, Map<String, Object>>> dataMap = dataList.parallelStream().map(stringObjectMap -> new Pair<>(String.valueOf(stringObjectMap.get(primaryKey)), stringObjectMap)).collect(Collectors.toList());
        elasticsearchService.nestedBatchInsert(request.getIndex(), databaseTableModel.getTable(), dataMap);
    }

    private List<Map<String, Object>> convertDateType(List<Map<String, Object>> source) {
        source.parallelStream().forEach(map -> map.forEach((key, value) -> {
            Optional.ofNullable(value).ifPresent(valueNotNull -> {
                if (valueNotNull instanceof Timestamp) {
                    map.put(key, LocalDateTime.ofInstant(((Timestamp) valueNotNull).toInstant(), ZoneId.systemDefault()));
                }
                if (valueNotNull instanceof Date) {
                    map.put(key,((Date)valueNotNull).toLocalDate());
                }
            });
        }));
        return source;
    }
}
