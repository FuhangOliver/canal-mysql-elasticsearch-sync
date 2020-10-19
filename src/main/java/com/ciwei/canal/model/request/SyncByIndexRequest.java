package com.ciwei.canal.model.request;


import javax.validation.constraints.NotBlank;

/**
 * @author fuhang
 * @description: 通过es索引，全量同步数据库配置
 * @date 2020/5/26 11:26
 */
public class SyncByIndexRequest {
    @NotBlank
    private String index;
    private Integer stepSize = 500;
    private Long from;
    private Long to;

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public Integer getStepSize() {
        return stepSize;
    }

    public void setStepSize(Integer stepSize) {
        this.stepSize = stepSize;
    }

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
}
