package cn.bounter.flink;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * 实时监控告警任务
 *
 * 链路结构：
 *  /B
 * A   /D
 *  \C
 *    \E
 *
 * 监控元数据：
 * A -> AB,AC
 * B -> AB
 * C -> AC,CD,CE
 * D -> CD
 * E -> CE
 *
 * Socket输入：(节点名称-节点编号-节点数据-traceId)
 * A-1-1-1
 * B-2-1-1
 * C-3-8-1
 * D-4-1-1
 * E-5-1-1
 *
 * 告警：C节点数据和其他节点不一样，所以C节点关联的链路（AC、CD、CE）会发出告警
 */
public class AlermJob {

    /**
     * Redis中的监控元数据，key:节点编码，value:节点关联的所有监控
     */
    private static Map<String, List<Monitor>> monitorsInRedis = new HashMap<>();

    /**
     * Redis中的持久化的监控记录，key:目标节点编码，value:监控记录
     */
    private static final Map<String, MonitorRecord> monitorRecordsInRedis = new HashMap<>();

    /**
     * 节点编号和节点名称映射器（方便打印节点名称，与业务逻辑无关）
     */
    private static final Map<String, String> codeNameMapper = new HashMap<>();

    public static void main(String[] args) throws Exception {
        //初始化节点编号名称映射器
        codeNameMapper.put("1", "A");
        codeNameMapper.put("2", "B");
        codeNameMapper.put("3", "C");
        codeNameMapper.put("4", "D");
        codeNameMapper.put("5", "E");

        /**
         * 初始化监控元数据，key:节点编码，value：节点关联的所有监控
         */
        Monitor AB = new Monitor().setSourceNodeCode("1").setTargetNodeCode("2");
        Monitor AC = new Monitor().setSourceNodeCode("1").setTargetNodeCode("3");
        Monitor CD = new Monitor().setSourceNodeCode("3").setTargetNodeCode("4");
        Monitor CE = new Monitor().setSourceNodeCode("3").setTargetNodeCode("5");
        monitorsInRedis.put("1", Arrays.asList(AB, AC));
        monitorsInRedis.put("2", Arrays.asList(AB));
        monitorsInRedis.put("3", Arrays.asList(AC, CD, CE));
        monitorsInRedis.put("4", Arrays.asList(CD));
        monitorsInRedis.put("5", Arrays.asList(CE));

        //获取运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //task需要做的事情
        DataStream<String> dataStream = env
                .socketTextStream("localhost", 8888)
                .map((MapFunction<String, Node>) value -> {
                    String[] nodeAttrs = value.split("-");
                    return new Node().setName(nodeAttrs[0]).setCode(nodeAttrs[1]).setData(nodeAttrs[2]).setTraceId(nodeAttrs[3]);
                })
                .keyBy(Node::getTraceId)
                .timeWindow(Time.seconds(30))
                .process(new ProcessWindowFunction<Node, String, String, TimeWindow>() {

                    @Override
                    public void process(String s, Context context, Iterable<Node> elements, Collector<String> out) throws Exception {
                        System.out.println("-------开始进行窗口聚合-------");
                        //没有聚合到数据时直接返回
                        if (!elements.iterator().hasNext()) {
                            return;
                        }

                        //打印时间窗口聚合到的所有节点数据
                        System.out.println("聚合的节点：");
                        elements.forEach(node -> System.out.println(node.getName()));

                        //等待匹配告警规则的监控记录
                        Set<MonitorRecord> matchSet = new HashSet<>();
                        //key:目标节点，value:目标节点对应的监控记录列表
                        Map<String, MonitorRecord> monitorRecordsInMemory = new HashMap<>();
                        //遍历聚合的节点
                        elements.forEach(node -> {
                            if (CollectionUtils.isNotEmpty(monitorsInRedis.get(node.getCode()))) {
                                //遍历节点关联的monitor
                                monitorsInRedis.get(node.getCode()).forEach(monitor -> {
                                    if (monitorRecordsInMemory.get(monitor.getTargetNodeCode()) != null) {
                                        // 内存中查找监控记录
                                        fillNodeInfo(monitorRecordsInMemory.get(monitor.getTargetNodeCode()), node, monitor);
                                        matchSet.add(monitorRecordsInMemory.get(monitor.getTargetNodeCode()));
                                        //删除已处理的记录
                                        monitorRecordsInMemory.remove(monitor.getTargetNodeCode());
                                    } else if (monitorRecordsInRedis.get(monitor.getTargetNodeCode()) != null) {
                                        // redis中查找监控记录
                                        fillNodeInfo(monitorRecordsInRedis.get(monitor.getTargetNodeCode()), node, monitor);
                                        matchSet.add(monitorRecordsInRedis.get(monitor.getTargetNodeCode()));
                                        //删除处理完的记录
                                        //TODO：redis删除；redis过期队列删除
                                        monitorRecordsInRedis.remove(monitor.getTargetNodeCode());
                                    } else {
                                        //创建监控记录
                                        //TODO：匹配监控条件
                                        MonitorRecord monitorRecord = new MonitorRecord();
                                        if (node.getCode().equals(monitor.getSourceNodeCode())) {
                                            //源节点先到
                                            monitorRecord.setSourceNode(node).setTargetNode(new Node().setCode(monitor.getTargetNodeCode()).setName(codeNameMapper.get(monitor.getTargetNodeCode())));
                                        } else if (node.getCode().equals(monitor.getTargetNodeCode())) {
                                            //目标节点先到
                                            monitorRecord.setSourceNode(new Node().setCode(monitor.getSourceNodeCode()).setName(codeNameMapper.get(monitor.getSourceNodeCode()))).setTargetNode(node);
                                        }
                                        monitorRecordsInMemory.put(monitor.getTargetNodeCode(), monitorRecord);
                                    }

                                    //遍历监控记录，匹配告警规则
                                    if (CollectionUtils.isNotEmpty(matchSet)) {
                                        matchSet.forEach(monitorRecord -> {
                                            //模拟告警规则
                                            //TODO：匹配告警规则
                                            if (!monitorRecord.getSourceNode().getData().equals(monitorRecord.getTargetNode().getData())) {
                                                //模拟发送告警
                                                System.err.println("发出一条告警信息，告警内容：[节点数据不一致]，源节点：[" + monitorRecord.getSourceNode().getName() + "]，节点数据：[" + monitorRecord.getSourceNode().getData() + "]，目标节点：[" + monitorRecord.getTargetNode().getName() + "]，节点数据：[" + monitorRecord.getTargetNode().getData() +"]");
                                            }
                                        });
                                    }
                                    //清理数据
                                    matchSet.clear();
                                });
                            }
                        });

                        //所有时间窗口的节点处理完后，如果内存中还有剩下的监控记录，则存到Redis; key:目标节点，value:监控记录
                        if (!monitorRecordsInMemory.isEmpty()) {
                            //遍历每个元素，保存到redis
                            monitorRecordsInMemory.forEach(monitorRecordsInRedis::put);
                        }
                        System.out.println("窗口关闭时Redis中的监控记录：" + (monitorRecordsInRedis.isEmpty() ? "无" : ""));
                        monitorRecordsInRedis.values().forEach(System.out::println);

                        System.out.println("-------窗口聚合结束-------");
                    }
                });

        //执行task
        env.execute("AlermJob");
    }

    /**
     * 填充节点信息
     * @param monitorRecord
     * @param node
     * @param monitor
     */
    private static void fillNodeInfo(MonitorRecord monitorRecord, Node node, Monitor monitor) {
        if (node.getCode().equals(monitor.getSourceNodeCode())) {
            monitorRecord.setSourceNode(node);
        } else if (node.getCode().equals(monitor.getTargetNodeCode())) {
            monitorRecord.setTargetNode(node);
        }
    }
}
