package com.ciwei.canal.dao;

import javafx.util.Pair;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

/**
 * @author fuhang
 * @description: 操作数据库的dao层
 * @date 2020/5/26 11:26
 */
@Mapper
public interface BaseDao {

    Map<String, Object> selectByPK(@Param("key") String key, @Param("value") Object value, @Param("databaseName") String databaseName, @Param("tableName") String tableName);

    List<Map<String, Object>> selectByPKs(@Param("key") String key, @Param("valueList") List<Object> valueList, @Param("databaseName") String databaseName, @Param("tableName") String tableName);

    List<Map<String, Object>> selectByPKsLockInShareMode(@Param("key") String key, @Param("valueList") List<Object> valueList, @Param("databaseName") String databaseName, @Param("tableName") String tableName);

    Long count(@Param("databaseName") String databaseName, @Param("tableName") String tableName);

    Long selectMaxPK(@Param("key") String key, @Param("databaseName") String databaseName, @Param("tableName") String tableName);

    Long selectMinPK(@Param("key") String key, @Param("databaseName") String databaseName, @Param("tableName") String tableName);

    List<Map<String, Object>> selectByPKInterval(@Param("key") String key, @Param("minPK") long minPK, @Param("maxPK") long maxPK, @Param("databaseName") String databaseName, @Param("tableName") String tableName);

    List<Map<String, Object>> selectByPKIntervalLockInShareMode(@Param("key") String key, @Param("minPK") long minPK, @Param("maxPK") long maxPK, @Param("databaseName") String databaseName, @Param("tableName") String tableName);

    List<Pair<String, String>> selectFieldValueDataType(@Param("databaseName") String databaseName, @Param("tableName") String tableName);
}
