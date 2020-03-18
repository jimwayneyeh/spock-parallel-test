package tw.jimwayneyeh.spock.spockparalleltest;

import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import software.amazon.kinesis.lifecycle.events.*;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

@Slf4j
@Setter
@Accessors(fluent = true)
public class MyRecordProcessor implements ShardRecordProcessor {
    private ForkJoinPool processorPool;

    @Override
    public void initialize(InitializationInput initializationInput) {
    }

    @Override
    public void processRecords(ProcessRecordsInput input) {
        try {
            processorPool.submit(() -> {
                return input.records().stream()
                        .collect(Collectors.groupingBy(KinesisClientRecord::partitionKey))
                        .values()
                        .parallelStream()
                        .map(records -> {
                            String lastSeq = StringUtils.EMPTY;
                            for(KinesisClientRecord record : records) {
                                lastSeq = processRecord(record);
                            }
                            return StringUtils.EMPTY;
                        }).collect(Collectors.toList());
            }).get();
        } catch (Throwable t) {
            log.error("Error occurred when submitting parallel tasks.", t);
        }
    }

    protected String processRecord(KinesisClientRecord record) {
        log.info("Process record: {}", record);
        return record.sequenceNumber();
    }

    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
    }
}
