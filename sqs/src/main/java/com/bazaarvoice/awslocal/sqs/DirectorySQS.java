package com.bazaarvoice.awslocal.sqs;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AbstractAmazonSQS;
import com.amazonaws.services.sqs.model.BatchResultErrorEntry;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchResult;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchResultEntry;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityResult;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResultEntry;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueResult;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.ListQueuesRequest;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.PurgeQueueResult;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.QueueNameExistsException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageBatchResultEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * This class is a basic implementation of AmazonSQS, intended as a substitute for local development/testing.
 */
public class DirectorySQS extends AbstractAmazonSQS {

    private static final Logger LOGGER = LoggerFactory.getLogger(DirectorySQS.class);

    private static final int AMAZON_DEFAULT_VISIBILITY = 30; // seconds
    private static final String BASE_ARN = "arn:aws:sqs:local:user:";

    private final File _rootDirectory;
    private final int _defaultVisibilitySeconds;

    private final Map<String, DirectorySQSQueue> _queuesByUrl = new HashMap<>();
    private final WatchService _queuesWatcher;

    public DirectorySQS(File rootDirectory)
            throws IOException {
        this(rootDirectory, AMAZON_DEFAULT_VISIBILITY);
    }

    public DirectorySQS(File rootDirectory, int defaultVisibilitySeconds) throws IOException {
        checkAccess(rootDirectory);
        _rootDirectory = rootDirectory;
        _defaultVisibilitySeconds = defaultVisibilitySeconds;
        _queuesWatcher = FileSystems.getDefault().newWatchService();
        startWatcher();
    }

    private void startWatcher() {
        final Thread thread = new Thread(() -> {
            while (true) {
                try {
                    final WatchKey key = _queuesWatcher.take();
                    for (WatchEvent<?> event : key.pollEvents()) {
                        handleWatcherEvent(event, (Path) key.watchable());
                    }
                    key.reset();
                } catch (ClosedWatchServiceException | InterruptedException e) {
                    LOGGER.debug("Watcher thread exiting: " + _rootDirectory);
                    break;
                }
            }
        });
        thread.setName("DirectorySQS-Watcher-Thread");
        thread.setDaemon(true);
        thread.start();
    }

    private void handleWatcherEvent(WatchEvent<?> event, Path parent) {
        final Path newPath = (Path)event.context();
        DirectorySQSQueue queueOfEvent = _queuesByUrl.get(parent.toUri().toString());
        if (queueOfEvent == null) {
            return; //some file was created somewhere but we don't know about the queue it's in
        }
        if (event.kind() == StandardWatchEventKinds.ENTRY_CREATE) {
            queueOfEvent.addFile(parent.resolve(newPath));
        }
    }

    private void checkAccess(File directory) {
        if (!directory.exists()) {
            //noinspection ResultOfMethodCallIgnored
            directory.mkdirs();
        }
        if (!directory.exists() || !directory.isDirectory() || !directory.canRead() || !directory.canWrite()) {
            throw new IllegalArgumentException("Unable to access directory: " + directory);
        }
    }

    private String saveQueue(DirectorySQSQueue queue) {
        final String queueUrl = queue.getQueuePath().toUri().toString();
        //don't panic, this is reentrant
        synchronized (_queuesByUrl) {
            if (!_queuesByUrl.containsKey(queueUrl)) {
                _queuesByUrl.put(queueUrl, queue);
            }
        }
        try {
            queue.getQueuePath().register(_queuesWatcher, StandardWatchEventKinds.ENTRY_CREATE);
        } catch (IOException e) {
            LOGGER.warn("problem registering queue with watcher " + queue.getQueuePath().toString(), e);
        }
        return queueUrl;
    }

    private DirectorySQSQueue getQueueFromUrl(String queueUrl, boolean remove) {
        synchronized (_queuesByUrl) {
            DirectorySQSQueue queue = remove ? _queuesByUrl.remove(queueUrl) : _queuesByUrl.get(queueUrl);
            if (queue == null) {
                try {
                    queue = new DirectorySQSQueue(Paths.get(new URI(queueUrl)));
                } catch (URISyntaxException e) {
                    throw new AmazonClientException("Invalid URI: " + queueUrl, e);
                } catch (FileNotFoundException e) {
                    throw new QueueDoesNotExistException("Invalid URI: " + queueUrl);
                } catch (IOException e) {
                    throw new AmazonServiceException("Error reading: " + queueUrl, e);
                }
                if (!remove)  {
                    saveQueue(queue);
                }
            }
            return queue;
        }
    }

    @Override
    public CreateQueueResult createQueue(CreateQueueRequest createQueueRequest) throws AmazonClientException {
        try {
            File topicFile = new File(_rootDirectory, createQueueRequest.getQueueName());
            if (topicFile.exists()) {
                throw new QueueNameExistsException("File exists: " + topicFile);
            }
            Files.createDirectory(topicFile.toPath());
            return new CreateQueueResult().withQueueUrl(saveQueue(new DirectorySQSQueue(topicFile.toPath())));
        } catch (IOException e) {
            throw new AmazonServiceException("could not create a queue named " + createQueueRequest.getQueueName(), e);
        }
    }

    //opens a SQS queue (for the specified file), puts it into the map, and returns the path (as the url result)
    @Override
    public GetQueueUrlResult getQueueUrl(GetQueueUrlRequest getQueueUrlRequest) throws AmazonClientException {
        try {
            File topicFile = new File(_rootDirectory, getQueueUrlRequest.getQueueName());
            if (!topicFile.exists()) {
                throw new QueueDoesNotExistException("could not find a file for queue named " + getQueueUrlRequest.getQueueName());
            }
            return new GetQueueUrlResult().withQueueUrl(saveQueue(new DirectorySQSQueue(topicFile.toPath())));
        } catch (IOException e) {
            throw new AmazonServiceException("could not get queue named " + getQueueUrlRequest.getQueueName(), e);
        }
    }

    @Override
    public SendMessageResult sendMessage(SendMessageRequest sendMessageRequest) throws AmazonClientException {
        DirectorySQSQueue queue = getQueueFromUrl(sendMessageRequest.getQueueUrl(), false);
        final int invisibilityDelay = Optional.ofNullable(sendMessageRequest.getDelaySeconds()).orElse(0);//0 is amazon spec default

        try {
            final Message message = queue.send(sendMessageRequest.getMessageBody(), invisibilityDelay);
            return new SendMessageResult().withMessageId(message.getMessageId()).withMD5OfMessageBody(message.getMD5OfBody());
        } catch (IOException e) {
            throw new AmazonServiceException("error sending message to " + queue.getQueuePath().toUri().toString(), e);
        }
    }

    @Override
    public ReceiveMessageResult receiveMessage(ReceiveMessageRequest receiveMessageRequest) throws AmazonClientException {
        DirectorySQSQueue queue = getQueueFromUrl(receiveMessageRequest.getQueueUrl(), false);

        //make sure we have a default for max number of messages.
        int maxNumberOfMessages = Optional.ofNullable(receiveMessageRequest.getMaxNumberOfMessages()).orElse(10); //10 is amazon spec default
        //and a default visibility timeout
        int visibilityTimeout = Optional.ofNullable(receiveMessageRequest.getVisibilityTimeout()).orElse(_defaultVisibilitySeconds);
        //also a wait time
        int waitTime = Optional.ofNullable(receiveMessageRequest.getWaitTimeSeconds()).orElse(0);
        if (waitTime < 0 || waitTime > 20) {
            throw new AmazonServiceException("wait time of " + waitTime + " is not between 0 and 20");
        }
        try {
            List<Message> messageList = queue.receive(maxNumberOfMessages, visibilityTimeout, waitTime);
            return new ReceiveMessageResult().withMessages(messageList);
        } catch (IOException e) {
            throw new AmazonServiceException("error reading messages from " + queue.getQueuePath().toUri().toString(), e);
        }
    }

    @Override
    public DeleteMessageResult deleteMessage(DeleteMessageRequest deleteMessageRequest) throws AmazonClientException {
        try {
            DirectorySQSQueue queue = getQueueFromUrl(deleteMessageRequest.getQueueUrl(), false);
            queue.delete(deleteMessageRequest.getReceiptHandle());
            return new DeleteMessageResult();
        } catch (IOException e) {
            throw new AmazonServiceException("error deleting message", e);
        }
    }

    @Override
    public DeleteMessageBatchResult deleteMessageBatch(DeleteMessageBatchRequest deleteMessageBatchRequest) throws AmazonClientException {
        DirectorySQSQueue queue = getQueueFromUrl(deleteMessageBatchRequest.getQueueUrl(), false);
        //lists for reporting
        List<BatchResultErrorEntry> batchResultErrorEntries = new ArrayList<>();
        List<DeleteMessageBatchResultEntry> batchResultEntries = new ArrayList<>();
        //attempt delete on each
        for (DeleteMessageBatchRequestEntry batchRequestEntry : deleteMessageBatchRequest.getEntries()) {
            try {
                queue.delete(batchRequestEntry.getReceiptHandle());
                batchResultEntries.add(new DeleteMessageBatchResultEntry().withId(batchRequestEntry.getId()));
            } catch (IOException e) {
                BatchResultErrorEntry batchResultErrorEntry = new BatchResultErrorEntry().
                        withSenderFault(true).
                        withId(batchRequestEntry.getId()).
                        withMessage(e.getMessage());
                batchResultErrorEntries.add(batchResultErrorEntry);
            }
        }
        return new DeleteMessageBatchResult().withFailed(batchResultErrorEntries).withSuccessful(batchResultEntries);
    }

    @Override
    public ChangeMessageVisibilityResult changeMessageVisibility(ChangeMessageVisibilityRequest changeMessageVisibilityRequest) throws AmazonClientException {
        try {
            DirectorySQSQueue queue = getQueueFromUrl(changeMessageVisibilityRequest.getQueueUrl(), false);
            queue.changeVisibility(changeMessageVisibilityRequest.getReceiptHandle(), changeMessageVisibilityRequest.getVisibilityTimeout());
            return new ChangeMessageVisibilityResult();
        } catch (IOException e) {
            throw new AmazonServiceException("error", e);
        }
    }

    @Override
    public ChangeMessageVisibilityBatchResult changeMessageVisibilityBatch(ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest) throws AmazonClientException {
        DirectorySQSQueue queue = getQueueFromUrl(changeMessageVisibilityBatchRequest.getQueueUrl(), false);
        //lists for reporting
        List<BatchResultErrorEntry> batchResultErrorEntries = new ArrayList<>();
        List<ChangeMessageVisibilityBatchResultEntry> batchResultEntries = new ArrayList<>();
        //attempt to change the visibility on each
        for (ChangeMessageVisibilityBatchRequestEntry batchRequestEntry : changeMessageVisibilityBatchRequest.getEntries()) {
            try {
                queue.changeVisibility(batchRequestEntry.getReceiptHandle(), batchRequestEntry.getVisibilityTimeout());
                batchResultEntries.add(new ChangeMessageVisibilityBatchResultEntry().withId(batchRequestEntry.getId()));
            } catch (Exception e) {
                BatchResultErrorEntry batchResultErrorEntry = new BatchResultErrorEntry().
                        withSenderFault(true).
                        withId(batchRequestEntry.getId()).
                        withMessage(e.getMessage());
                batchResultErrorEntries.add(batchResultErrorEntry);
            }
        }
        return new ChangeMessageVisibilityBatchResult().withFailed(batchResultErrorEntries).withSuccessful(batchResultEntries);
    }

    @Override
    public DeleteQueueResult deleteQueue(DeleteQueueRequest deleteQueueRequest) throws AmazonClientException {
        final DirectorySQSQueue queue = getQueueFromUrl(deleteQueueRequest.getQueueUrl(), true);
        try {
            Files.delete(queue.getQueuePath());
            return new DeleteQueueResult();
        } catch (IOException e) {
            throw new AmazonServiceException("Could not delete queue: " + queue.getQueuePath());
        }
    }

    @Override
    public ListQueuesResult listQueues(ListQueuesRequest listQueuesRequest) throws AmazonClientException {
        List<String> queueUrls = new ArrayList<>(_queuesByUrl.size());
        try (DirectoryStream<Path> queuePaths = Files.newDirectoryStream(_rootDirectory.toPath())) {
            for (Path queuePath : queuePaths) {
                if (listQueuesRequest.getQueueNamePrefix() == null || queuePath.getFileName().toString().startsWith(listQueuesRequest.getQueueNamePrefix())) {
                    queueUrls.add(queuePath.toUri().toString());
                }
            }
        } catch (IOException e) {
            throw new AmazonServiceException("could not get queue list", e);
        }
        return new ListQueuesResult().withQueueUrls(queueUrls);
    }

    @Override
    public SendMessageBatchResult sendMessageBatch(SendMessageBatchRequest sendMessageBatchRequest) throws AmazonClientException {
        DirectorySQSQueue queue = getQueueFromUrl(sendMessageBatchRequest.getQueueUrl(), false);
        //lists for reporting
        List<BatchResultErrorEntry> batchResultErrorEntries = new ArrayList<>();
        List<SendMessageBatchResultEntry> batchResultEntries = new ArrayList<>();
        //attempt to change the visibility on each
        for (SendMessageBatchRequestEntry batchRequestEntry : sendMessageBatchRequest.getEntries()) {
            try {
                final int invisibilityDelay = Optional.ofNullable(batchRequestEntry.getDelaySeconds()).orElse(0);//0 is amazon spec default
                Message sentMessage = queue.send(batchRequestEntry.getMessageBody(), invisibilityDelay);
                batchResultEntries.add(new SendMessageBatchResultEntry().
                        withId(batchRequestEntry.getId()).
                        withMessageId(sentMessage.getMessageId()).
                        withMD5OfMessageBody(sentMessage.getMD5OfBody()));
            } catch (IOException e) {
                BatchResultErrorEntry batchResultErrorEntry = new BatchResultErrorEntry().
                        withSenderFault(false).
                        withId(batchRequestEntry.getId()).
                        withMessage(e.getMessage());
                batchResultErrorEntries.add(batchResultErrorEntry);
            }
        }
        return new SendMessageBatchResult().
                withFailed(batchResultErrorEntries).
                withSuccessful(batchResultEntries);
    }

    public void shutdown() {
        try {
            _queuesWatcher.close();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public GetQueueAttributesResult getQueueAttributes(GetQueueAttributesRequest getQueueAttributesRequest) throws AmazonClientException {
        DirectorySQSQueue queue = getQueueFromUrl(getQueueAttributesRequest.getQueueUrl(), false);
        Map<String, String> attributes = new HashMap<>();
        List<String> unsupported = new ArrayList<>();

        for (String attribute : getQueueAttributesRequest.getAttributeNames()) {
            switch (attribute) {
                case "QueueArn":
                    attributes.put("QueueArn", BASE_ARN + queue.getQueuePath().getFileName());
                    break;
                case "All":
                case "ApproximateNumberOfMessages":
                case "ApproximateNumberOfMessagesNotVisible":
                case "VisibilityTimeout":
                case "CreatedTimestamp":
                case "LastModifiedTimestamp":
                case "Policy":
                case "MaximumMessageSize":
                case "MessageRetentionPeriod":
                case "ApproximateNumberOfMessagesDelayed":
                case "DelaySeconds":
                case "ReceiveMessageWaitTimeSeconds":
                default:
                    unsupported.add(attribute);
                    break;
            }
        }

        if (!unsupported.isEmpty()) {
            throw new UnsupportedOperationException("attributes not implemented: " + unsupported);
        }

        return new GetQueueAttributesResult().withAttributes(attributes);
    }

    @Override
    public PurgeQueueResult purgeQueue(PurgeQueueRequest purgeQueueRequest) {
        DirectorySQSQueue queue = getQueueFromUrl(purgeQueueRequest.getQueueUrl(), false);
        try {
            queue.purge();
            return new PurgeQueueResult();
        } catch (IOException e) {
            throw new AmazonServiceException("Could not purge queue: " + queue.getQueuePath());
        }
    }
}

