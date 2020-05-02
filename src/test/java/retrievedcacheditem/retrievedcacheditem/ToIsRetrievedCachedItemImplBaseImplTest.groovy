package retrievedcacheditem.retrievedcacheditem

import id.id.IsId
import id.output.IsOutput
import io.grpc.StatusRuntimeException
import io.grpc.inprocess.InProcessChannelBuilder
import main.Test
import retrievedcacheditem.input.IsInput
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.TimeUnit

class ToIsRetrievedCachedItemImplBaseImplTest extends Specification {

    @Shared
    def blockingStub = ToIsRetrievedCachedItemGrpc.newBlockingStub(InProcessChannelBuilder.forName(ToIsRetrievedCachedItemGrpc.SERVICE_NAME).usePlaintext().build()).withDeadlineAfter(30, TimeUnit.SECONDS).withWaitForReady()

    def setupSpec() {
        Test.before()
    }

    def """Should not allow empty"""() {

        setup:
        def request = NotRetrievedCachedItem.newBuilder().build()

        when:
        blockingStub.produce(request)

        then:
        thrown StatusRuntimeException
    }


    def """Should not allow empty key"""() {
        setup:
        def request = NotRetrievedCachedItem.newBuilder()
                .setIsInput(IsInput.newBuilder()
                        .setIsId(IsId.newBuilder()
                                .setIsOutput(IsOutput.newBuilder().build())
                                .build())
                        .build())
                .build();

        when:
        blockingStub.produce(request)

        then:
        def exception = thrown StatusRuntimeException
        exception.message == "INVALID_ARGUMENT: 422"
    }
}
