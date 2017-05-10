package com.bazaarvoice.awslocal.sqs;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiptHandleIsInvalidException;
import com.amazonaws.util.BinaryUtils;
import com.amazonaws.util.Md5Utils;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class manages a single queue that is persisted in a particular directory.
 *
 * It tracks the state of the directory in-memory, which may get stale if other instances are
 *  operating on the same directory.  To deal with this, it always validates its "known" messages,
 *  and claims them with atomic file system operations.  Finally, it relies on a process external
 *  to this class to watch the directory and inform it of new messages that may be available.
 */
public class DirectorySQSQueue {

    private static final Logger LOGGER = LoggerFactory.getLogger(DirectorySQSQueue.class);

    private static final String SUFFIX = ".msg";

    private final Path _path;
    private final PriorityBlockingQueue<MessageAccessor> _messageQueue = new PriorityBlockingQueue<>();
    private final AtomicInteger _counter = new AtomicInteger(1);

    public DirectorySQSQueue(Path path)
            throws IOException {
        _path = path;
        if (!Files.isDirectory(path)) {
            throw new FileNotFoundException("Invalid path: " + path);
        }

        try (final DirectoryStream<Path> stream = Files.newDirectoryStream(_path)) {
            for (Path file : stream) {
                addFile(file);
            }
        }
    }

    public Path getQueuePath() {
        return _path;
    }

    public void addFile(Path messageFile) {
        if (!Files.isRegularFile(messageFile) || !messageFile.getFileName().toString().endsWith(SUFFIX)) {
            return;
        }
        try {
            _messageQueue.offer(toMessageAccessor(messageFile));
        } catch (Exception e) {
            LOGGER.warn("Error parsing filename: " + messageFile, e);
        }
    }

    public Message send(String messageBody, int invisibilityDelay) throws IOException {
        final String messageId = RandomStringUtils.randomAlphanumeric(100);
        final MessageAccessor accessor = new MessageAccessor(messageId).withVisibilityTimeoutSeconds(invisibilityDelay);

        final byte[] bytes = messageBody.getBytes("UTF-8");
        tryAtomicRename(writeToTempFile(messageId, bytes), accessor);
        _messageQueue.offer(accessor);
        return new Message().withMessageId(messageId).withBody(messageBody).withMD5OfBody(getMD5(bytes));
    }

    private String getMD5(byte[] bytes) {
        return BinaryUtils.toHex(Md5Utils.computeMD5Hash(bytes));
    }

    private boolean tryAtomicRename(Path source, MessageAccessor accessor)
            throws IOException {
        try {
            Files.move(source, accessor.getPath(), StandardCopyOption.ATOMIC_MOVE);
            return true;
        } catch (NoSuchFileException e) {
            return false;
        }
    }

    public List<Message> receive(int maximumNumberOfMessages, int visibilityTimeout, int waitTime) throws IOException {
        List<MessageAccessor> claimed = Lists.newArrayListWithCapacity(maximumNumberOfMessages);

        final long endTimeMillis = System.currentTimeMillis() + waitTime * 1000;
        boolean tryClaimAgain = false;
        long currentTimeMillis = System.currentTimeMillis();
        //at least one claiming attempt should be made, and it should be repeated as long as there is time left for it
        //(or until it actually succeeds, of course)
        do {
            tryClaimAgain = claimTo(claimed, visibilityTimeout, endTimeMillis - currentTimeMillis);
        } while (claimed.isEmpty() && ((currentTimeMillis = System.currentTimeMillis()) < endTimeMillis));

        while (claimed.size() < maximumNumberOfMessages && tryClaimAgain) {
            tryClaimAgain = claimTo(claimed, visibilityTimeout, 0);
        }

        List<Message> messages = Lists.newArrayListWithCapacity(claimed.size());
        for (MessageAccessor accessor : claimed) {
            _messageQueue.offer(accessor);
            messages.add(new Message().
                    withMessageId(accessor.getMessageId()).
                    withBody(accessor.getMessageBody()).
                    withMD5OfBody(accessor.getMessageMD5()).
                    withReceiptHandle(accessor.getPath().toString()));
        }
        return messages;
    }

    /**
     * @return whether or not the loop calling this method should attempt calling this again.
     */
    private boolean claimTo(List<MessageAccessor> claimed, int visibilityTimeout, long waitTimeMillis) throws IOException {
        final MessageAccessor accessor;
        if (waitTimeMillis > 0) {
            try {
                accessor = _messageQueue.poll(waitTimeMillis, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                return false;
            }
        } else {
            accessor = _messageQueue.poll();
        }
        if (accessor == null) {
            return false; // nothing available
        }
        if (!Files.isReadable(accessor.getPath())) {
            return true; // no longer available
        }
        if (!accessor.isVisible()) {
            _messageQueue.offer(accessor);
            return false; // not yet available
        }

        final MessageAccessor futureAccessor = accessor.withVisibilityTimeoutSeconds(visibilityTimeout);
        if (tryAtomicRename(accessor.getPath(), futureAccessor)) {
            claimed.add(futureAccessor);
        }
        return true;
    }

    public void changeVisibility(String receiptHandle, int visibilityTimeout)
            throws IOException {
        final Path path = toValidPath(receiptHandle);
        final MessageAccessor accessor = toMessageAccessor(path);
        final MessageAccessor futureAccessor = accessor.withVisibilityTimeoutSeconds(visibilityTimeout);
        if (tryAtomicRename(accessor.getPath(), futureAccessor)) {
            _messageQueue.offer(futureAccessor);
        }
    }

    public void delete(String receiptHandle) throws IOException {
        final Path path = toValidPath(receiptHandle);
        Files.deleteIfExists(path);
    }

    public void purge() throws IOException {
        MessageAccessor accessor;
        while ((accessor = _messageQueue.poll()) != null) {
            Files.deleteIfExists(accessor.getPath());
        }
    }

    private Path toValidPath(String receiptHandle) {
        final Path path = Paths.get(receiptHandle);
        if (!Files.exists(path)) {
            throw new ReceiptHandleIsInvalidException("Invalid handle: " + receiptHandle);
        }
        return path;
    }

    private Path writeToTempFile(String messageId, byte[] bytes)
            throws IOException {
        final Path file = Files.createTempFile(_path, messageId, ".tmp");
        try (final OutputStream outputStream = Files.newOutputStream(file, StandardOpenOption.WRITE)) {
            outputStream.write(bytes);
        }
        return file;
    }

    private MessageAccessor toMessageAccessor(Path file) {
        final String filename = _path.relativize(file).toString();
        final String baseName = StringUtils.removeEnd(filename, SUFFIX);
        final String[] parts = StringUtils.split(baseName, "-", 3);
        return new MessageAccessor(parts[0], Integer.parseInt(parts[1]), Long.parseLong(parts[2]));
    }

    private class MessageAccessor implements Comparable<MessageAccessor> {
        private final String _messageId;
        private final int _number;
        private final long _visibilityTimeMillis;
        private transient String _messageBody;
        private transient String _messageMD5;

        public MessageAccessor(String messageId) {
            this(messageId, _counter.getAndIncrement(), System.currentTimeMillis());
        }

        public MessageAccessor(String messageId, int number, long visibilityTimeMillis) {
            _messageId = messageId;
            _number = number;
            _visibilityTimeMillis = visibilityTimeMillis;
        }

        public String getMessageId() {
            return _messageId;
        }

        @Override
        public int compareTo(MessageAccessor that) {
            return ComparisonChain.start()
                    .compare(this._visibilityTimeMillis, that._visibilityTimeMillis)
                    .compare(this._messageId, that._messageId)
                    .compare(this._number, that._number)
                    .result();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof MessageAccessor)) {
                return false;
            }

            MessageAccessor that = (MessageAccessor) o;
            return _messageId.equals(that._messageId) && this._number == that._number && _visibilityTimeMillis == that._visibilityTimeMillis;
        }

        @Override
        public int hashCode() {
            return _messageId.hashCode();
        }

        public boolean isVisible() {
            return _visibilityTimeMillis <= System.currentTimeMillis();
        }

        public MessageAccessor withVisibilityTimeoutSeconds(int visibilityTimeout) {
            return new MessageAccessor(_messageId, _number, System.currentTimeMillis() + 1000 * visibilityTimeout);
        }

        public Path getPath() {
            return _path.resolve(_messageId + "-" + _number + "-" + _visibilityTimeMillis + SUFFIX);
        }

        public synchronized String getMessageBody()
                throws IOException {
            if (_messageBody == null) {
                final byte[] bytes = Files.readAllBytes(getPath());
                _messageBody = new String(bytes, "UTF-8");
                _messageMD5 = getMD5(bytes);
            }
            return _messageBody;
        }

        public synchronized String getMessageMD5()
                throws IOException {
            getMessageBody();
            return _messageMD5;
        }
    }
}

