package com.ciwei.canal.service.impl;

import com.ciwei.canal.model.DatabaseTableModel;
import com.ciwei.canal.model.IndexTypeModel;
import com.ciwei.canal.service.MultiMappingService;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * @author fuhang
 * @description: 处理mysql和es的字段映射问题，一对多
 * @date 2020/5/26 11:26
 */
@Service
@PropertySource("classpath:multi-mapping.properties")
@ConfigurationProperties
public class MultiMappingServiceImpl implements MultiMappingService, InitializingBean {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
    private static final DateTimeFormatter DAY_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private Map<String, Converter> mysqlTypeElasticsearchTypeMapping;

    private Map<String, String> mysqlTypeElasticsearchTextTypeMapping;

    /**
     * mysql多个数据库对应一个索引
     */
    private Map<List<String>, String> multiDbEsMapping;
    private BiMap<List<DatabaseTableModel>, IndexTypeModel> multiDbEsBiMapping;
    private Map<List<String>, String> multiTablePrimaryKeyMap;

    @Override
    public Map<List<String>, String> getMultiTablePrimaryKeyMap() {
        return multiTablePrimaryKeyMap;
    }

    @Override
    public String getMultiTablePrimaryKey(String dataBase, String table, String index) {
        Map<String, String> stringStringMap = new HashMap<>();
        multiTablePrimaryKeyMap.forEach((key, value) -> {
            String dataBaseTable = new StringBuilder().append(dataBase).append(".").append(table).toString();
            String primaryKey = key.contains(dataBaseTable) ? value :
                    new StringBuilder().append(index).append(".").append("id").toString();
            String[] keyStrings = StringUtils.split(primaryKey, ".");
            stringStringMap.put(keyStrings[0], keyStrings[1]);
        });
        return stringStringMap.get(index);
    }

    @Override
    public void setMultiTablePrimaryKeyMap(Map<List<String>, String> multiTablePrimaryKeyMap) {
        this.multiTablePrimaryKeyMap = multiTablePrimaryKeyMap;
    }

    @Override
    public List<IndexTypeModel> getIndexType(DatabaseTableModel databaseTableModel) {
        List<IndexTypeModel> indexTypeModelList = Lists.newArrayList();
        multiDbEsBiMapping.forEach((key, value) -> {
            if (key.contains(databaseTableModel)) {
                indexTypeModelList.add(value);
            }
        });
        return indexTypeModelList;
    }

    @Override
    public List<DatabaseTableModel> getMultiDatabaseTableModel(IndexTypeModel indexTypeModel) {
        return multiDbEsBiMapping.inverse().get(indexTypeModel);
    }

    @Override
    public Object getElasticsearchTypeObject(String mysqlType, String data) {
        Optional<Entry<String, Converter>> result = mysqlTypeElasticsearchTypeMapping.entrySet().parallelStream().filter(entry ->
                (mysqlType.toLowerCase().contains("date") || mysqlType.toLowerCase().contains("int")) ? mysqlType.toLowerCase().equals(entry.getKey()) :
                        mysqlType.toLowerCase().contains(entry.getKey()))
                .findFirst();
        return (result.isPresent() ? result.get().getValue() : (Converter) data1 -> data1).convert(data);
    }

    @Override
    public String getElasticsearchType(String mysqlType) {
        Optional<Entry<String, String>> result = mysqlTypeElasticsearchTextTypeMapping.entrySet().parallelStream().filter(entry ->
                (mysqlType.toLowerCase().contains("date") || mysqlType.toLowerCase().contains("int")) ? mysqlType.toLowerCase().equals(entry.getKey()) :
                        mysqlType.toLowerCase().contains(entry.getKey()))
                .findFirst();
        return (result.isPresent() ? result.get().getValue() : "text");
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        multiDbEsBiMapping = HashBiMap.create();
        multiDbEsMapping.forEach((key, value) -> {
            List<DatabaseTableModel> databaseTableModelList = key.stream().map(dbTable -> {
                String[] keyStrings = StringUtils.split(dbTable, ".");
                DatabaseTableModel databaseTableModel = new DatabaseTableModel(keyStrings[0], keyStrings[1]);
                return databaseTableModel;
            }).collect(Collectors.toList());
            multiDbEsBiMapping.put(databaseTableModelList, new IndexTypeModel(value));
        });

        mysqlTypeElasticsearchTypeMapping = Maps.newHashMap();
        mysqlTypeElasticsearchTypeMapping.put("char", data -> data);
        mysqlTypeElasticsearchTypeMapping.put("text", data -> data);
        mysqlTypeElasticsearchTypeMapping.put("blob", data -> data);
        mysqlTypeElasticsearchTypeMapping.put("int", Integer::valueOf);
        mysqlTypeElasticsearchTypeMapping.put("bigint", Long::valueOf);
        mysqlTypeElasticsearchTypeMapping.put("datetime", data -> LocalDateTime.parse(data, FORMATTER));
        mysqlTypeElasticsearchTypeMapping.put("time", data -> LocalDateTime.parse(data, FORMATTER));
        mysqlTypeElasticsearchTypeMapping.put("date", data -> LocalDate.parse(data, DAY_FORMATTER));
        mysqlTypeElasticsearchTypeMapping.put("float", Double::valueOf);
        mysqlTypeElasticsearchTypeMapping.put("double", Double::valueOf);
        mysqlTypeElasticsearchTypeMapping.put("decimal", Double::valueOf);


        mysqlTypeElasticsearchTextTypeMapping = Maps.newHashMap();
        mysqlTypeElasticsearchTextTypeMapping.put("char", "text");
        mysqlTypeElasticsearchTextTypeMapping.put("varchar", "text");
        mysqlTypeElasticsearchTextTypeMapping.put("text", "text");
        mysqlTypeElasticsearchTextTypeMapping.put("blob", "binary");
        mysqlTypeElasticsearchTextTypeMapping.put("int", "long");
        mysqlTypeElasticsearchTextTypeMapping.put("bigint", "long");
        mysqlTypeElasticsearchTextTypeMapping.put("datetime", "date");
        mysqlTypeElasticsearchTextTypeMapping.put("time", "date");
        mysqlTypeElasticsearchTextTypeMapping.put("date", "date");
        mysqlTypeElasticsearchTextTypeMapping.put("float", "float");
        mysqlTypeElasticsearchTextTypeMapping.put("double", "double");
        mysqlTypeElasticsearchTextTypeMapping.put("decimal", "double");
    }

    private interface Converter {
        /**
         * 字符串转为es可以识别的日期
         *
         * @param data 字符串类型的日期
         * @return
         */
        Object convert(String data);
    }

    public Map<List<String>, String> getMultiDbEsMapping() {
        return multiDbEsMapping;
    }

    public void setMultiDbEsMapping(Map<List<String>, String> multiDbEsMapping) {
        this.multiDbEsMapping = multiDbEsMapping;
    }

    public BiMap<List<DatabaseTableModel>, IndexTypeModel> getMultiDbEsBiMapping() {
        return multiDbEsBiMapping;
    }

    public void setMultiDbEsBiMapping(BiMap<List<DatabaseTableModel>, IndexTypeModel> multiDbEsBiMapping) {
        this.multiDbEsBiMapping = multiDbEsBiMapping;
    }
}
