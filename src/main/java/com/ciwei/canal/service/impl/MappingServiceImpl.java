package com.ciwei.canal.service.impl;

import com.ciwei.canal.model.DatabaseTableModel;
import com.ciwei.canal.model.IndexTypeModel;
import com.ciwei.canal.service.MappingService;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

/**
 * @author fuhang
 * @description: 处理mysql和es的字段映射问题
 * @date 2020/5/26 11:26
 */
@Service
@PropertySource("classpath:mapping.properties")
@ConfigurationProperties
public class MappingServiceImpl implements MappingService, InitializingBean {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
    private static final DateTimeFormatter DAY_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private Map<String, String> dbEsMapping;
    private BiMap<DatabaseTableModel, IndexTypeModel> dbEsBiMapping;
    private Map<String, String> tablePrimaryKeyMap;
    private Map<String, Converter> mysqlTypeElasticsearchTypeMapping;

    @Override
    public Map<String, String> getTablePrimaryKeyMap() {
        return tablePrimaryKeyMap;
    }

    @Override
    public void setTablePrimaryKeyMap(Map<String, String> tablePrimaryKeyMap) {
        this.tablePrimaryKeyMap = tablePrimaryKeyMap;
    }

    @Override
    public IndexTypeModel getIndexType(DatabaseTableModel databaseTableModel) {
        return dbEsBiMapping.get(databaseTableModel);
    }

    @Override
    public DatabaseTableModel getDatabaseTableModel(IndexTypeModel indexTypeModel) {
        return dbEsBiMapping.inverse().get(indexTypeModel);
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
    public void afterPropertiesSet() throws Exception {
        dbEsBiMapping = HashBiMap.create();
        dbEsMapping.forEach((key, value) -> {
            String[] keyStrings = StringUtils.split(key, ".");
            dbEsBiMapping.put(new DatabaseTableModel(keyStrings[0], keyStrings[1]), new IndexTypeModel(value));
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
    }

    public Map<String, String> getDbEsMapping() {
        return dbEsMapping;
    }

    public void setDbEsMapping(Map<String, String> dbEsMapping) {
        this.dbEsMapping = dbEsMapping;
    }

    private interface Converter {
        Object convert(String data);
    }
}
