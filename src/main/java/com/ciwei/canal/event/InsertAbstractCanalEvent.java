package com.ciwei.canal.event;

import com.alibaba.otter.canal.protocol.CanalEntry.Entry;

/**
 * @author fuhang
 * @description: 插入事件
 * @date 2020/5/26 11:26
 */
public class InsertAbstractCanalEvent extends AbstractCanalEvent {
    /**
     * Create a new ApplicationEvent.
     *
     * @param source the object on which the event initially occurred (never {@code null})
     */
    public InsertAbstractCanalEvent(Entry source) {
        super(source);
    }
}
