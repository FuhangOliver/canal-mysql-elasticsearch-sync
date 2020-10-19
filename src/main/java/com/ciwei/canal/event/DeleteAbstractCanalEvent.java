package com.ciwei.canal.event;

import com.alibaba.otter.canal.protocol.CanalEntry.Entry;

/**
 * @author fuhang
 * @description: 删除事件
 * @date 2020/5/26 11:26
 */
public class DeleteAbstractCanalEvent extends AbstractCanalEvent {
    /**
     * Create a new ApplicationEvent.
     *
     * @param source the object on which the event initially occurred (never {@code null})
     */
    public DeleteAbstractCanalEvent(Entry source) {
        super(source);
    }
}
