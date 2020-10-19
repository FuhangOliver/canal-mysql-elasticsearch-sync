package com.ciwei.canal.model;

import com.google.common.base.Objects;

/**
 * @author fuhang
 * @description: es的索引
 * @date 2020/5/26 11:26
 */
public class IndexTypeModel {
    private String index;

    public IndexTypeModel() {
    }

    public IndexTypeModel(String index) {
        this.index = index;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexTypeModel that = (IndexTypeModel) o;
        return Objects.equal(index, that.index);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(index);
    }
}
