package com.ciwei.canal.model.request;


import javax.validation.constraints.NotBlank;

/**
 * @author fuhang
 * @description: 通过mysql表，全量同步数据库配置
 * @date 2020/5/26 11:26
 */
public class SyncByTableRequest {
    @NotBlank
    private String database;
    @NotBlank
    private String table;
    private Integer stepSize = 500;
    private Long from;
    private Long to;

    public Long getFrom() {
        return from;
    }

    public void setFrom(Long from) {
        this.from = from;
    }

    public Long getTo() {
        return to;
    }

    public void setTo(Long to) {
        this.to = to;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Integer getStepSize() {
        return stepSize;
    }

    public void setStepSize(Integer stepSize) {
        this.stepSize = stepSize;
    }
}
