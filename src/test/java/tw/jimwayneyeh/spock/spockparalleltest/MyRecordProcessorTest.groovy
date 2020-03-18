package tw.jimwayneyeh.spock.spockparalleltest

import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput
import software.amazon.kinesis.retrieval.KinesisClientRecord
import spock.lang.Specification

import java.util.concurrent.ForkJoinPool

class MyRecordProcessorTest extends Specification {

    def "test record processing"() {
        given:
        MyRecordProcessor recordProcessor = Spy(MyRecordProcessor, constructorArgs: [])
                .processorPool(ForkJoinPool.commonPool())

        ProcessRecordsInput input = Mock(ProcessRecordsInput) {
            records() >> [
                    Mock(KinesisClientRecord) {
                        partitionKey() >> "test"
                    }
            ]
        }

        when:
        recordProcessor.processRecords(input)

        then:
        1 * recordProcessor.processRecord(_)
    }
}
