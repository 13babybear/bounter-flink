package cn.bounter.flink;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * 简化的节点
 */
@Data
@Accessors(chain = true)
public class Node {

    /**
     * 节点名称
     */
    private String name;

    /**
     * 节点编号
     */
    private String code;

    /**
     * 节点数据
     */
    private String data;

    /**
     * 链路id
     */
    private String traceId;
}
