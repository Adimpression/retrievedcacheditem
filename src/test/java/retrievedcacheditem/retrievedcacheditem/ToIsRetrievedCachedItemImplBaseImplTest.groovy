package retrievedcacheditem.retrievedcacheditem

import com.google.protobuf.ByteString
import id.id.IsId
import id.output.IsOutput
import io.grpc.StatusRuntimeException
import io.grpc.inprocess.InProcessChannelBuilder
import main.Test
import retrievedcacheditem.input.IsInput
import spock.lang.Shared
import spock.lang.Specification
import storedcacheditem.storedcacheditem.NotStoredCachedItem
import storedcacheditem.storedcacheditem.ToIsStoredCachedItemGrpc

import java.util.concurrent.TimeUnit

class ToIsRetrievedCachedItemImplBaseImplTest extends Specification {


    @Shared
    def retrieve

    @Shared
    def store

    def setupSpec() {
        Test.before()
        store = ToIsStoredCachedItemGrpc.newBlockingStub(InProcessChannelBuilder.forName(ToIsStoredCachedItemGrpc.SERVICE_NAME).usePlaintext().build()).withDeadlineAfter(1, TimeUnit.MINUTES).withWaitForReady()
        retrieve = ToIsRetrievedCachedItemGrpc.newBlockingStub(InProcessChannelBuilder.forName(ToIsRetrievedCachedItemGrpc.SERVICE_NAME).usePlaintext().build()).withDeadlineAfter(1, TimeUnit.MINUTES).withWaitForReady()
    }


    def """Should not allow empty"""() {

        setup:
        def request = NotRetrievedCachedItem.newBuilder().build()

        when:
        retrieve.produce(request)

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
        retrieve.produce(request)

        then:
        def exception = thrown StatusRuntimeException
        exception.message == "INVALID_ARGUMENT: 422"
    }

    def """Should throw an error on missing key"""() {
        setup:
        def request = NotRetrievedCachedItem.newBuilder()
                .setIsInput(IsInput.newBuilder()
                        .setIsId(IsId.newBuilder()
                                .setIsOutput(IsOutput.newBuilder()
                                        .setIsStringValue(String.valueOf(System.currentTimeMillis()))
                                        .build())
                                .build())
                        .build())
                .build();

        when:
        retrieve.produce(request)

        then:
        def exception = thrown StatusRuntimeException
        exception.message == "INVALID_ARGUMENT: 404"
    }


    def """Should produce value on existing key"""() {

        setup:
        def key = System.currentTimeMillis()
        def storeRequest = NotStoredCachedItem.newBuilder()
                .setIsInput(storedcacheditem.input.IsInput.newBuilder()
                        .setIsId(IsId.newBuilder()
                                .setIsOutput(IsOutput.newBuilder()
                                        .setIsStringValue(String.valueOf(key))
                                        .build())
                                .build())
                        .setIsUseLockBoolean(true)
                        .setIsItemBytes(ByteString.copyFrom(String.valueOf(key % 1000), "UTF-8"))
                        .build())
                .build()
        def request = NotRetrievedCachedItem.newBuilder()
                .setIsInput(IsInput.newBuilder()
                        .setIsId(IsId.newBuilder()
                                .setIsOutput(IsOutput.newBuilder()
                                        .setIsStringValue(String.valueOf(key))
                                        .build())
                                .build())
                        .setIsUseLockBoolean(true)
                        .build())
                .build();

        when:
        store.produce(storeRequest)

        and:
        def isRetrievedCachedItem = retrieve.produce(request)

        then:
        Long.valueOf(isRetrievedCachedItem.getIsOutput().getIsItemBytes().toString("UTF-8")) == key % 1000

    }

}
