package com.ciwei.canal.service.impl;

import com.ciwei.canal.dao.BaseDao;
import com.ciwei.canal.model.DatabaseTableModel;
import com.ciwei.canal.model.IndexTypeModel;
import com.ciwei.canal.service.ElasticsearchService;
import com.ciwei.canal.service.MultiMappingService;
import com.ciwei.canal.util.JsonUtil;
import com.google.common.collect.Lists;
import javafx.util.Pair;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author fuhang
 * @description: 操作es的RestHighLevelClient
 * @date 2020/5/26 11:26
 */
@Service
public class ElasticsearchServiceImpl implements ElasticsearchService {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchServiceImpl.class);

    @Resource
    protected RestHighLevelClient client;

    @Resource
    private MultiMappingService multiMappingService;

    @Resource
    private BaseDao baseDao;

    @Override
    public void insertById(String index, String id, Map<String, Object> dataMap) {
        IndexRequest request = new IndexRequest(index).id(id).source(dataMap);
        try {
            IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
            logger.info("elasticsearch插入成功, index= {} , data= {} , result: {}", index, JsonUtil.toJson(dataMap), indexResponse.toString());
        } catch (IOException e) {
            logger.error("elasticsearch插入错误, index=" + index + ", data=" + JsonUtil.toJson(dataMap), e);
        }
    }

    @Override
    public void batchInsertById(String index, Map<String, Map<String, Object>> idDataMap) {
        BulkRequest bulkRequest = new BulkRequest();
        idDataMap.forEach((id, dataMap) -> {
            IndexRequest request = new IndexRequest(index).id(id).source(dataMap);
            bulkRequest.add(request);
        });
        try {
            BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            if (bulkResponse.hasFailures()) {
                logger.error("elasticsearch批量插入错误, index=" + index + ", data=" + JsonUtil.toJson(idDataMap) + ", cause:" + bulkResponse.buildFailureMessage());
            }
        } catch (IOException e) {
            logger.error("elasticsearch批量插入错误, index=" + index + ", data=" + JsonUtil.toJson(idDataMap), e);
        }
    }

    @Override
    public void update(String index, String id, Map<String, Object> dataMap) {
        this.insertById(index, id, dataMap);
    }

    @Override
    public void deleteById(String index, String id) {
        DeleteRequest request = new DeleteRequest(index, String.valueOf(id));
        try {
            DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);
            logger.info("elasticsearch删除成功, index=" + index + ", id=" + id + ", result:" + deleteResponse.toString());
        } catch (IOException e) {
            logger.error("elasticsearch插入错误, index=" + index + ", id=" + id, e);
        }
    }

    @Override
    public boolean isExist(String index, String id) {
        GetRequest getRequest = new GetRequest(index).id(id);
        try {
            return client.exists(getRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            logger.error("elasticsearch判断文档存在错误, index=" + index + ", id=" + id, e);
            return false;
        }
    }

    @Override
    public void syncMysqlToEsSuper(String index, String id, String tableName, Map<String, Object> dataMap) {
        SyncParameter syncParameter = new SyncParameter(index, id, tableName, dataMap);
        Consumer<SyncParameter> result = syncMysqlToEsDispatcher.get(this.getMysqlToEsSyncType(syncParameter));
        Optional.ofNullable(result).ifPresent(res -> res.accept(syncParameter));
    }

    @Override
    public void nestedInsert(String index, String id, String tableName, Map<String, Object> dataMap) {
        try {
            IndexResponse indexResponse = client.index(this.getNestedInsertRequest(index, id, tableName, dataMap), RequestOptions.DEFAULT);
            logger.info("elasticsearch nested插入成功, index= {} , data= {} , result: {}", index, JsonUtil.toJson(dataMap), indexResponse.toString());
        } catch (IOException e) {
            logger.error("elasticsearch nested插入失败, index=" + index + ", data=" + JsonUtil.toJson(dataMap), e);
        }
    }

    private IndexRequest getNestedInsertRequest(String index, String id, String tableName, Map<String, Object> dataMap) {
        Map<String, Object> getResult = getById(index, id);
        IndexRequest request = new IndexRequest(index).id(id);
        List<Map<String, Object>> data = Lists.newArrayList();
        data.add(dataMap);
        Map<String, Object> script = new HashMap<>(60);
        script.put(tableName, data);
        Optional.ofNullable(getResult).ifPresent(getResultNotNull -> script.putAll(getResultNotNull));
        request.source(script);
        return request;
    }

    @Override
    public void nestedUpdate(String index, String id, String tableName, Map<String, Object> dataMap) {
        try {
            UpdateResponse updateResponse = client.update(this.getNestedUpdateRequest(index, id, tableName, dataMap), RequestOptions.DEFAULT);
            logger.info("elasticsearch nested更新成功, index= {} , data= {} , result: {}", index, JsonUtil.toJson(dataMap), updateResponse.toString());
        } catch (IOException e) {
            logger.error("elasticsearch nested更新失败, index=" + index + ", data=" + JsonUtil.toJson(dataMap), e);
        }
    }

    private UpdateRequest getNestedUpdateRequest(String index, String id, String tableName, Map<String, Object> dataMap) {
        List<String> sourceList = dataMap.entrySet().stream().map(entry ->
                new StringBuilder().append("e.").append(entry.getKey()).append(" = params.").append(entry.getKey()).toString()
        ).collect(Collectors.toList());
        String sourceStr = sourceList.stream().map(source -> new StringBuilder().append(source).append(";")).collect(Collectors.joining(""));
        UpdateRequest request = new UpdateRequest(index, id);
        String scriptStr = new StringBuilder().append("for(e in ctx._source.").append(tableName).append("){if (e.id == ")
                .append("params.id").append(") {").append(sourceStr).append("}}").toString();
        Script inline = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptStr, dataMap);
        request.script(inline);
        return request;
    }

    @Override
    public void nestedUpdateByQuery(String index, String queryField, String tableName, Map<String, Object> dataMap) {
        UpdateByQueryRequest request = new UpdateByQueryRequest(index);
        // 修改的条件
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.nestedQuery(tableName, QueryBuilders.termQuery(queryField,dataMap.get(queryField)), ScoreMode.Max));
        request.setQuery(queryBuilder);
        // 更新最大文档数
        request.setSize(100);
        // 批次大小
        request.setBatchSize(1000);
        // 并行
        request.setSlices(2);
        // 使用滚动参数来控制“搜索上下文”存活的时间
        request.setScroll(TimeValue.timeValueMinutes(10));
        // 超时
        request.setTimeout(TimeValue.timeValueMinutes(2));
        // 刷新索引
        request.setRefresh(true);
        // 更新数据，设置修改脚本
        List<String> sourceList = dataMap.entrySet().stream().map(entry ->
                new StringBuilder().append("e.").append(entry.getKey()).append(" = params.").append(entry.getKey()).toString()
        ).collect(Collectors.toList());
        String sourceStr = sourceList.stream().map(source -> new StringBuilder().append(source).append(";")).collect(Collectors.joining(""));
        String scriptStr = new StringBuilder().append("for(e in ctx._source.").append(tableName).append("){if (e.id == ")
                .append("params.id").append(") {").append(sourceStr).append("}}").toString();
        Script inline = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptStr, dataMap);
        request.setScript(inline);
        try {
            BulkByScrollResponse response = client.updateByQuery(request, RequestOptions.DEFAULT);
            logger.info("elasticsearch nestedUpdateByQuery成功, index= {} , data= {} , result: {}", index, JsonUtil.toJson(dataMap), response.toString());
        } catch (IOException e) {
            logger.error("elasticsearch nestedUpdateByQuery错误, index=" + index + ", data=" + JsonUtil.toJson(dataMap), e);
        }
    }

    @Override
    public void nestedAdd(String index, String id, String tableName, Map<String, Object> dataMap) {
        try {
            UpdateResponse updateResponse = client.update(this.getNestedAddRequest(index, id, tableName, dataMap), RequestOptions.DEFAULT);
            logger.info("elasticsearch nested新增成功, index= {} , data= {} , result: {}", index, JsonUtil.toJson(dataMap), updateResponse.toString());
        } catch (IOException e) {
            logger.error("elasticsearch nested新增错误, index=" + index + ", data=" + JsonUtil.toJson(dataMap), e);
        }
    }

    private UpdateRequest getNestedAddRequest(String index, String id, String tableName, Map<String, Object> dataMap) {
        UpdateRequest request = new UpdateRequest(index, id);
        String scriptStr = new StringBuilder().append("ctx._source.").append(tableName).append(".add(params.dataMap)").toString();
        Map<String, Object> parameters = Collections.singletonMap("dataMap", dataMap);
        Script inline = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptStr, parameters);
        request.script(inline);
        return request;
    }

    @Override
    public void nestedDelete(String index, String id, String tableName, Map<String, Object> dataMap) {
        UpdateRequest request = new UpdateRequest(index, id);
        String scriptStr = new StringBuilder().append("ctx._source.").append(tableName).append(".removeIf(it -> it.id == ").append("params.id").append(");").toString();
        Map<String, Object> parameters = Collections.singletonMap("id", dataMap.get("id"));
        Script inline = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptStr, parameters);
        request.script(inline);
        try {
            UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
            logger.info("elasticsearch nested删除成功, index= {} , data= {} , result: {}", index, JsonUtil.toJson(inline), updateResponse.toString());
        } catch (IOException e) {
            logger.error("elasticsearch nested删除错误, index=" + index + ", data=" + JsonUtil.toJson(inline), e);
        }
    }

    @Override
    public boolean nestedPathExist(String index, String id, String tableName) {
        QueryBuilder pathExist = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("_id", id))
                .must(QueryBuilders.nestedQuery(tableName, QueryBuilders.existsQuery(tableName), ScoreMode.Max));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(pathExist);
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(searchSourceBuilder);
        searchRequest.indices(index);
        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = searchResponse.getHits();
            return hits.getTotalHits().value > 0L ? true : false;
        } catch (IOException e) {
            logger.error("elasticsearch nested判断失败, index=" + index + ", _id=" + id, e);
            return false;
        }
    }

    @Override
    public boolean checkIndexExists(String index) {
        GetIndexRequest request = new GetIndexRequest(index);
        try {
            return client.indices().exists(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            logger.error("elasticsearch判断索引异常, index= {},exception：{}", index, e);
        }
        return false;
    }

    @Override
    public void nestedBatchInsert(String index, String tableName, List<Pair<String, Map<String, Object>>> idDataMap) {
        BulkRequest bulkRequest = new BulkRequest();
        idDataMap.stream().forEach(stringMapPair -> {
            String id = stringMapPair.getKey();
            Map<String, Object> dataMap = stringMapPair.getValue();
            SyncParameter syncParameter = new SyncParameter(index, id, tableName, dataMap);
            Function<SyncParameter, DocWriteRequest> result = syncMysqlToEsRequestDispatcher.get(this.getMysqlToEsSyncType(syncParameter));
            DocWriteRequest docWriteRequest = Optional.ofNullable(result).map(res -> res.apply(syncParameter))
                    .orElseThrow(() -> new IllegalArgumentException(String.format("syncMysqlToEsRequestDispatcher无法获取table = %s的同步请求", tableName)));
            bulkRequest.add(docWriteRequest);
        });
        try {
            BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            if (bulkResponse.hasFailures()) {
                logger.error("elasticsearch nested批量插入错误, index=" + index + ", data=" + JsonUtil.toJson(idDataMap) + ", cause:" + bulkResponse.buildFailureMessage());
            }
        } catch (IOException e) {
            logger.error("elasticsearch nested批量插入错误, index=" + index + ", data=" + JsonUtil.toJson(idDataMap), e);
        }
    }

    @Override
    public void createIndex(String index) {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
        createIndexRequest.settings(Settings.builder()
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 1)
        );
        try {
            createIndexRequest.mapping(this.generateMapping(index));
            CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            logger.info("elasticsearch创建索引成功, index = {}，result = {}", index, createIndexResponse.toString());
        } catch (IOException e) {
            logger.info("elasticsearch创建索引失败，index = {},exception = {}", index, e);
        }
    }

    private XContentBuilder generateMapping(String index) throws IOException {
        List<DatabaseTableModel> databaseTableModelList = this.multiMappingService.getMultiDatabaseTableModel(new IndexTypeModel(index));
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.field("dynamic", "strict");
            mapping.startObject("properties");
            {
                for (DatabaseTableModel databaseTableModel : databaseTableModelList) {
                    List<Pair<String, String>> fieldValueDataTypeMap = this.baseDao.selectFieldValueDataType(databaseTableModel.getDatabase(), databaseTableModel.getTable());
                    mapping.startObject(databaseTableModel.getTable()).field("type", "nested");
                    {
                        mapping.startObject("properties");
                        {
                            for (Pair<String, String> pair : fieldValueDataTypeMap) {
                                String type = this.multiMappingService.getElasticsearchType(pair.getValue());
//                                String format = "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis";
                                mapping.startObject(pair.getKey());
                                {
                                    mapping.field("type", type);
//                                    if ("date".equals(type)) {
//                                        mapping.field("format", format);
//                                    }
                                }
                                mapping.endObject();
                            }
                        }
                        mapping.endObject();
                    }
                    mapping.endObject();
                }
            }
            mapping.endObject();
        }
        mapping.endObject();
        return mapping;
    }

    /**
     * 根据索引和文档id获取文档
     */
    private Map<String, Object> getById(String index, String id) {
        GetRequest getRequest = new GetRequest(index, id);
        try {
            GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
            Map<String, Object> getResult = response.getSource();
            return getResult;
        } catch (IOException e) {
            logger.error("elasticsearch nested获取失败, index=" + index + ", id=" + id, e);
            return null;
        }
    }

    /**
     * 判断文档的嵌套结构是否存在
     */
    private boolean nestedDocExist(String index, String id, String tableName, String nestedId) {
        QueryBuilder pathExist = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("_id", id))
                .must(QueryBuilders.nestedQuery(tableName,
                        QueryBuilders.boolQuery()
                                .must(QueryBuilders.termQuery(tableName + ".id", nestedId)), ScoreMode.Max));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(pathExist);
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(searchSourceBuilder);
        searchRequest.indices(index);
        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = searchResponse.getHits();
            return hits.getTotalHits().value > 0L ? true : false;
        } catch (IOException e) {
            logger.error("elasticsearch nested判断Doc失败, index=" + index + ", _id=" + id, e);
            return false;
        }
    }

    /**
     * 新的数据进来进行条件判断，后决定同步到es的方式：
     * 1、索引是否存在 --> 文档是否存在 --> 文档嵌套是否存在 --> 嵌套文档数组是否已存在该条数据
     * 2、对以上的数据条件进行分发
     */
    private Map<Integer, Consumer<SyncParameter>> syncMysqlToEsDispatcher = new HashMap<>();

    /**
     * 新的数据进来进行条件判断，后决定同步到es的方式，这个方法只组装请求的request。
     * 1、索引是否存在 --> 文档是否存在 --> 文档嵌套是否存在 --> 嵌套文档数组是否已存在该条数据
     * 2、根据新数据的情况来组装请求体
     */
    private Map<Integer, Function<SyncParameter, DocWriteRequest>> syncMysqlToEsRequestDispatcher = new HashMap<>();

    @PostConstruct
    public void syncMysqlToEsDispatcherInit() {
        this.syncMysqlToEsDispatcher.put(1, syncParameter -> {
            this.createIndex(syncParameter.getIndex());
            this.nestedInsert(syncParameter.getIndex(), syncParameter.getId(), syncParameter.getTableName(), syncParameter.getDataMap());
        });
        this.syncMysqlToEsDispatcher.put(2, syncParameter -> this.nestedInsert(syncParameter.getIndex(), syncParameter.getId(), syncParameter.getTableName(), syncParameter.getDataMap()));
        this.syncMysqlToEsDispatcher.put(3, syncParameter -> this.nestedAdd(syncParameter.getIndex(), syncParameter.getId(), syncParameter.getTableName(), syncParameter.getDataMap()));
        this.syncMysqlToEsDispatcher.put(4, syncParameter -> this.nestedUpdate(syncParameter.getIndex(), syncParameter.getId(), syncParameter.getTableName(), syncParameter.getDataMap()));
    }

    @PostConstruct
    public void syncMysqlToEsRequestDispatcherInit() {
        this.syncMysqlToEsRequestDispatcher.put(1, syncParameter -> {
            this.createIndex(syncParameter.getIndex());
            return this.getNestedInsertRequest(syncParameter.getIndex(), syncParameter.getId(), syncParameter.getTableName(), syncParameter.getDataMap());
        });
        this.syncMysqlToEsRequestDispatcher.put(2, syncParameter -> this.getNestedInsertRequest(syncParameter.getIndex(), syncParameter.getId(), syncParameter.getTableName(), syncParameter.getDataMap()));
        this.syncMysqlToEsRequestDispatcher.put(3, syncParameter -> this.getNestedAddRequest(syncParameter.getIndex(), syncParameter.getId(), syncParameter.getTableName(), syncParameter.getDataMap()));
        this.syncMysqlToEsRequestDispatcher.put(4, syncParameter -> this.getNestedUpdateRequest(syncParameter.getIndex(), syncParameter.getId(), syncParameter.getTableName(), syncParameter.getDataMap()));
    }

    /**
     * 判断数据同步的类型
     * 索引不存在 1 ---> 先创建索引，然后直接插入
     * 索引存在，路径不存在 2 ---> 先查询再追加插入
     * 索引存在，路径存在，文档不存在 3 ---> nestedAdd
     * 索引存在，路径存在，文档存在 4 ---> nestedUpdate
     */
    private Integer getMysqlToEsSyncType(SyncParameter syncParameter) {
        return this.checkIndexExists(syncParameter.getIndex())
                ? (this.nestedPathExist(syncParameter.getIndex(), syncParameter.getId(), syncParameter.getTableName())
                ? (this.nestedDocExist(syncParameter.getIndex(), syncParameter.getId(), syncParameter.getTableName(), String.valueOf(syncParameter.getDataMap().get("id")))
                ? 4 : 3) : 2) : 1;
    }

    private class SyncParameter {
        private String index;
        private String id;
        private String tableName;
        private Map<String, Object> dataMap;

        public SyncParameter(String index, String id, String tableName, Map<String, Object> dataMap) {
            this.index = index;
            this.id = id;
            this.tableName = tableName;
            this.dataMap = dataMap;
        }

        public String getIndex() {
            return index;
        }

        public void setIndex(String index) {
            this.index = index;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public Map<String, Object> getDataMap() {
            return dataMap;
        }

        public void setDataMap(Map<String, Object> dataMap) {
            this.dataMap = dataMap;
        }
    }
}
