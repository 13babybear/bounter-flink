package cn.bounter.flink;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * 监控元数据
 */
@Data
@Accessors(chain = true)
public class Monitor {

    private Long id;

    /**
     * 源节点编码
     */
    private String sourceNodeCode;

    /**
     * 目标节点编码
     */
    private String targetNodeCode;
}
