package com.ciwei.canal.service.impl;

import com.ciwei.canal.dao.BaseDao;
import com.ciwei.canal.model.DatabaseTableModel;
import com.ciwei.canal.model.IndexTypeModel;
import com.ciwei.canal.model.request.SyncByIndexRequest;
import com.ciwei.canal.model.request.SyncByTableRequest;
import com.ciwei.canal.service.MappingService;
import com.ciwei.canal.service.MultiMappingService;
import com.ciwei.canal.service.SyncService;
import com.ciwei.canal.service.TransactionalService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;

/**
 * @author fuhang
 * @description: 异步全量同步数据
 * @date 2020/5/26 11:26
 */
@Service
public class SyncServiceImpl implements SyncService, InitializingBean, DisposableBean {
    private static final Logger logger = LoggerFactory.getLogger(SyncServiceImpl.class);
    /**
     * 使用线程池控制并发数量
     */
    private ExecutorService cachedThreadPool;

    @Resource
    private BaseDao baseDao;

    @Resource
    private MappingService mappingService;

    @Resource
    private MultiMappingService multiMappingService;

    @Resource
    private TransactionalService transactionalService;

    @Override
    public boolean syncByTable(SyncByTableRequest request) {
        IndexTypeModel indexTypeModel = mappingService.getIndexType(new DatabaseTableModel(request.getDatabase(), request.getTable()));
        String primaryKey = Optional.ofNullable(mappingService.getTablePrimaryKeyMap().get(request.getDatabase() + "." + request.getTable())).orElse("id");
        if (indexTypeModel == null) {
            throw new IllegalArgumentException(String.format("配置文件中缺失database=%s和table=%s所对应的index和type的映射配置", request.getDatabase(), request.getTable()));
        }
        long minPK = Optional.ofNullable(request.getFrom()).orElse(baseDao.selectMinPK(primaryKey, request.getDatabase(), request.getTable()));
        long maxPK = Optional.ofNullable(request.getTo()).orElse(baseDao.selectMaxPK(primaryKey, request.getDatabase(), request.getTable()));
        this.cachedThreadPool.submit(() -> {
            try {
                for (long i = minPK; i <= maxPK + request.getStepSize(); i += request.getStepSize()) {
                    this.transactionalService.batchInsertElasticsearch(request, primaryKey, i, i + request.getStepSize(), indexTypeModel);
                    logger.info(String.format("当前同步pk=%s，总共total=%s，进度=%s%%", i, maxPK, new BigDecimal(i * 100).divide(new BigDecimal(maxPK), 3, BigDecimal.ROUND_HALF_UP)));
                }
            } catch (Exception e) {
                logger.error("syncByTable --> 批量转换并插入Elasticsearch异常", e);
            }
        });
        return true;
    }

    @Override
    public boolean syncByElasticsearchIndex(SyncByIndexRequest request) {
        /**
         * 通过索引获取对应的数据库和对应的表。根据id来分段，很多的数据中间有删除的，这里不能使用多线程，因为有多个表的数据需要同步，会发生es的更新冲突的问题
         */
        List<DatabaseTableModel> databaseTableModelList = multiMappingService.getMultiDatabaseTableModel(new IndexTypeModel(request.getIndex()));
        Optional.ofNullable(databaseTableModelList).orElseThrow(() -> new IllegalArgumentException(String.format("配置文件中缺失index=%s所对应的database和table的映射配置", request.getIndex())));
        databaseTableModelList.stream().forEach(databaseTableModel -> {
            String indexKey = Optional.ofNullable(multiMappingService.getMultiTablePrimaryKey(databaseTableModel.getDatabase(), databaseTableModel.getTable(), request.getIndex())).orElse("id");
            long minPK = Optional.ofNullable(request.getFrom()).orElse(baseDao.selectMinPK(indexKey, databaseTableModel.getDatabase(), databaseTableModel.getTable()));
            long maxPK = Optional.ofNullable(request.getTo()).orElse(baseDao.selectMaxPK(indexKey, databaseTableModel.getDatabase(), databaseTableModel.getTable()));
            try {
                for (long i = minPK; i <= maxPK + request.getStepSize(); i += request.getStepSize()) {
                    this.transactionalService.batchInsertElasticsearchByIndex(request, indexKey, i, i + request.getStepSize(), databaseTableModel);
                    logger.info(String.format("当前同步pk=%s，总共total=%s，进度=%s%%", i, maxPK, new BigDecimal(i * 100).divide(new BigDecimal(maxPK), 3, BigDecimal.ROUND_HALF_UP)));
                }
            } catch (Exception e) {
                logger.error("syncByElasticsearchIndex --> 批量转换并插入Elasticsearch异常", e);
            }
        });
        return true;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("sync-pool-%d").build();
        this.cachedThreadPool = new ThreadPoolExecutor(5, 5,
                0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), namedThreadFactory);
    }

    @Override
    public void destroy() throws Exception {
        if (cachedThreadPool != null) {
            cachedThreadPool.shutdown();
        }
    }
}
