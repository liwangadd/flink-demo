package cn.windylee.flink.trigger;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class MyCountTrigger<W extends Window> extends Trigger<Long, W> {

    private long maxCount;
    private long maxSum;
    private final ReducingStateDescriptor<Long> countDesc;
    private final ReducingStateDescriptor<Long> sumDesc;

    private MyCountTrigger(long maxCount, long maxSum) {
        this.countDesc = new ReducingStateDescriptor<>("count", new Sum(), LongSerializer.INSTANCE);
        this.sumDesc = new ReducingStateDescriptor<>("sum", new Sum(), LongSerializer.INSTANCE);
        this.maxCount = maxCount;
        this.maxSum = maxSum;
    }

    @Override
    public TriggerResult onElement(Long element, long timestamp, W window, TriggerContext ctx) throws Exception {
        ReducingState<Long> count = ctx.getPartitionedState(this.countDesc);
        ReducingState<Long> sum = ctx.getPartitionedState(this.sumDesc);
        count.add(1L);
        sum.add(element);
        if (count.get() >= maxCount || sum.get() >= maxSum) {
            count.clear();
            sum.clear();
            return TriggerResult.FIRE_AND_PURGE;
        } else {
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long element, W window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long element, W window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        ctx.getPartitionedState(this.countDesc).clear();
        ctx.getPartitionedState(this.sumDesc).clear();
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(W window, OnMergeContext ctx) throws Exception {
        ctx.mergePartitionedState(this.countDesc);
        ctx.mergePartitionedState(this.sumDesc);
    }

    public static <W extends Window> MyCountTrigger<W> of(long maxCount, long maxSum) {
        return new MyCountTrigger<>(maxCount, maxSum);
    }

    private static class Sum implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        private Sum() {
        }

        public Long reduce(Long value1, Long value2) throws Exception {
            return value1 + value2;
        }
    }
}
