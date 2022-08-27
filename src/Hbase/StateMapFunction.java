package com.zzml.flinklearn.doitedu;

/**
 * @CopyRigth: com.zzml.flink
 * @ClassName:StateMapFunction
 * @Auther: zzml
 * @Description: 自定义数据源，用于测试operatorState
 * @Date: 2022/7/23 17:04
 * @Version: v1.0
 * @ModifyDate:
 */


import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

/**
 * 要使用operator state，需要让用户自己的Function类去实现 CheckpointedFunction
 * 然后在其中的 方法initializeState 中，去拿到operator state 存储器
 */
public class StateMapFunction implements MapFunction<String, String>, CheckpointedFunction {

    ListState<String> listState;

    /**
     * 正常的MapFunction的处理逻辑方法
     * @param value 输入数据
     * @return
     * @throws Exception
     */
    @Override
    public String map(String value) throws Exception {

        /**
         * 故意埋一个异常，来测试task级别自动容错效果
         */
        if (value.equals("x") && RandomUtils.nextInt(1, 15) % 4 == 0)
            throw new Exception("出异常了！！！");

        // 将数据插入到状态存储器中，
        listState.add(value);

        // 然后拼接历史以来的字符串
        Iterable<String> strings = listState.get();
        StringBuilder stringBuilder = new StringBuilder();
        for (String string : strings) {
            stringBuilder.append(string);
        }

        return stringBuilder.toString();
    }

    /**
     * 系统对状态数据做快照（持久化）时会调用的方法，用户利用这个方法，在持久化前，对状态数据做一些操控
     * @param context the context for drawing a snapshot of the operator
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

        // System.out.println("checkpoint 触发了，checkpointId : " +context.getCheckpointId());

    }

    /**
     * 算子任务在启动之初，会调用下面的方法，来为用户进行状态数据初始化
     * @param context the context for initializing the operator
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        // 从方法提供的context中拿到一个算子状态存储器
        OperatorStateStore operatorStateStore = context.getOperatorStateStore();

        // 算子状态存储器，只提供list数据结构来为用户存储数据
        ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<>("Strings", String.class); // 定义一个状态存储结构描述器

        // getListState方法，在task失败后，task自动重启时，会帮用户自动加载最近一次的快照状态数据
        // 如果是job重启，则不会自动加载此前的快照状态数据
        listState = operatorStateStore.getListState(listStateDescriptor);

        /**
         * unionListState 和普通 ListState的区别：
         * unionListState的快照存储数据，在系统重启后，list数据的重分配模式为： 广播模式； 在每个subtask上都拥有一份完整的数据
         * ListState的快照存储数据，在系统重启后，list数据的重分配模式为： round-robin； 轮询平均分配
         */
        //ListState<String> unionListState = operatorStateStore.getUnionListState(stateDescriptor);


    }
}
