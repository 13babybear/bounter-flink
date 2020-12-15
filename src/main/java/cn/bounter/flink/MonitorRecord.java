package cn.bounter.flink;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * 简化的监控记录
 */
@Data
@Accessors(chain = true)
public class MonitorRecord {

    /**
     * 源节点
     */
    private Node sourceNode;

    /**
     * 目标节点
     */
    private Node targetNode;

    @Override
    public String toString() {
        return sourceNode.getName() + targetNode.getName();
    }

}
