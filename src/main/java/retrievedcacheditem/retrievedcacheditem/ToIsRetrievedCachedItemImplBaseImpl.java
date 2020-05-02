package retrievedcacheditem.retrievedcacheditem;

import com.google.protobuf.ByteString;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import id.output.IsOutput;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import retrievedcacheditem.input.IsInput;

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
            final IsOutput isIdisOutput;

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

            isIdisOutput = isInput.getIsId()
                    .getIsOutput();
            if (isIdisOutput
                    .getIsStringValue().isEmpty()) {
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("422"));
            }

            responseObserver.onNext(IsRetrievedCachedItem.newBuilder()
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
