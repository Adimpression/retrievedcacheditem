package retrievedcacheditem.retrievedcacheditem;

import com.google.protobuf.ByteString;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import retrievedcacheditem.input.IsInput;
import retrievedcacheditem.output.IsOutput;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ToIsRetrievedCachedItemImplBaseImpl extends ToIsRetrievedCachedItemGrpc.ToIsRetrievedCachedItemImplBase {

    private final Logger logger;
    private final HazelcastInstance hzInstance;
    private final Map<String, ByteString> general;
    private final ILock lock;

    public ToIsRetrievedCachedItemImplBaseImpl() {
        logger = Logger.getLogger(getClass().getName());
        logger.info("starting");

        logger.info("starting hazelcast");
        hzInstance = Hazelcast.newHazelcastInstance();
        logger.info("started hazelcast");

        logger.info("starting general map: hazelcast");
        general = hzInstance.getMap("general");
        logger.info("started general map: hazelcast");

        logger.info("starting general lock: hazelcast");
        lock = hzInstance.getLock("general");
        logger.info("started general lock: hazelcast");

        logger.info("started");
    }

    @Override
    public void produce(final NotRetrievedCachedItem request, final StreamObserver<IsRetrievedCachedItem> responseObserver) {
        try {
            final IsInput isInput;
            final String isStringValue;

            if (!request.hasIsInput()) {
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("422"));
            }

            isInput = request.getIsInput();
            if (!isInput.hasIsId()) {
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("422"));
            }

            if (!isInput.getIsId()
                    .hasIsOutput()) {
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("422"));
            }

            isStringValue = isInput.getIsId()
                    .getIsOutput()
                    .getIsStringValue();
            if (isStringValue
                    .isEmpty()) {
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("422"));
            }

            final ByteString bytes;

            lock.lock();
            try {
                bytes = general.get(isStringValue);
            } finally {
                lock.unlock();
            }

            if (bytes == null) {
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("404"));
            }

            responseObserver.onNext(IsRetrievedCachedItem.newBuilder()
                    .setIsOutput(IsOutput.newBuilder()
                            .setIsItemBytes(bytes)
                            .build())
                    .build());

            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.log(Level.SEVERE,
                    e.getMessage(),
                    e);
            responseObserver.onError(e);
        }
    }
}
