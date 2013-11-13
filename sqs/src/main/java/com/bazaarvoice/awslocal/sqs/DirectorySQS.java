package com.bazaarvoice.awslocal.sqs;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.*;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class is a basic implementation of AmazonSQS, intended as a substitute for local development/testing.
 */
public class DirectorySQS implements AmazonSQS {

    private static final Logger LOGGER = LoggerFactory.getLogger(DirectorySQS.class);

    private static final int AMAZON_DEFAULT_VISIBILITY = 30; // seconds
    private static final String BASE_ARN = "arn:aws:sqs:local:user:";

    private final File _rootDirectory;
    private final int _defaultVisibilitySeconds;

    private final Map<String, DirectorySQSQueue> _queuesByUrl = Maps.newHashMap();
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

    private void startWatcher() throws IOException {
        final Thread thread = new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        final WatchKey key = _queuesWatcher.take();
                        for (WatchEvent<?> event : key.pollEvents()) {
                            handleWatcherEvent(event, (Path) key.watchable());
                        }
                        key.reset();
                    } catch (IOException e) {
                        LOGGER.warn("Error watching: " + _rootDirectory, e);
                    } catch (ClosedWatchServiceException | InterruptedException e) {
                        LOGGER.debug("Watcher thread exiting: " + _rootDirectory);
                        break;
                    }
                }
            }
        };
        thread.setName("DirectorySQS-Watcher-Thread");
        thread.setDaemon(true);
        thread.start();
    }

    private void handleWatcherEvent(WatchEvent<?> event, Path parent) throws IOException {
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
    public CreateQueueResult createQueue(CreateQueueRequest createQueueRequest) throws AmazonServiceException, AmazonClientException {
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
    public GetQueueUrlResult getQueueUrl(GetQueueUrlRequest getQueueUrlRequest) throws AmazonServiceException, AmazonClientException {
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

    public SendMessageResult sendMessage(SendMessageRequest sendMessageRequest) throws AmazonServiceException, AmazonClientException {
        DirectorySQSQueue queue = getQueueFromUrl(sendMessageRequest.getQueueUrl(), false);
        final int invisibilityDelay = Objects.firstNonNull(sendMessageRequest.getDelaySeconds(), 0);//0 is amazon spec default

        try {
            final Message message = queue.send(sendMessageRequest.getMessageBody(), invisibilityDelay);
            return new SendMessageResult().withMessageId(message.getMessageId()).withMD5OfMessageBody(message.getMD5OfBody());
        } catch (IOException e) {
            throw new AmazonServiceException("error sending message to " + queue.getQueuePath().toUri().toString(), e);
        }
    }

    public ReceiveMessageResult receiveMessage(ReceiveMessageRequest receiveMessageRequest) throws AmazonServiceException, AmazonClientException {
        DirectorySQSQueue queue = getQueueFromUrl(receiveMessageRequest.getQueueUrl(), false);

        //make sure we have a default for max number of messages.
        int maxNumberOfMessages = Objects.firstNonNull(receiveMessageRequest.getMaxNumberOfMessages(), 10); //10 is amazon spec default
        //and a default visibility timeout
        int visibilityTimeout = Objects.firstNonNull(receiveMessageRequest.getVisibilityTimeout(), _defaultVisibilitySeconds);
        //also a wait time
        int waitTime = Objects.firstNonNull(receiveMessageRequest.getWaitTimeSeconds(), 0);
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

    public void deleteMessage(DeleteMessageRequest deleteMessageRequest) throws AmazonServiceException, AmazonClientException {
        try {
            DirectorySQSQueue queue = getQueueFromUrl(deleteMessageRequest.getQueueUrl(), false);
            queue.delete(deleteMessageRequest.getReceiptHandle());
        } catch (IOException e) {
            throw new AmazonServiceException("error deleting message", e);
        }
    }

    public DeleteMessageBatchResult deleteMessageBatch(DeleteMessageBatchRequest deleteMessageBatchRequest) throws AmazonServiceException, AmazonClientException {
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

    public void changeMessageVisibility(ChangeMessageVisibilityRequest changeMessageVisibilityRequest) throws AmazonServiceException, AmazonClientException {
        try {
            DirectorySQSQueue queue = getQueueFromUrl(changeMessageVisibilityRequest.getQueueUrl(), false);
            queue.changeVisibility(changeMessageVisibilityRequest.getReceiptHandle(), changeMessageVisibilityRequest.getVisibilityTimeout());
        } catch (IOException e) {
            throw new AmazonServiceException("error", e);
        }
    }

    public ChangeMessageVisibilityBatchResult changeMessageVisibilityBatch(ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest) throws AmazonServiceException, AmazonClientException {
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

    public void deleteQueue(DeleteQueueRequest deleteQueueRequest) throws AmazonServiceException, AmazonClientException {
        final DirectorySQSQueue queue = getQueueFromUrl(deleteQueueRequest.getQueueUrl(), true);
        try {
            Files.delete(queue.getQueuePath());
        } catch (IOException e) {
            throw new AmazonServiceException("Could not delete queue: " + queue.getQueuePath());
        }
    }

    public ListQueuesResult listQueues() throws AmazonServiceException, AmazonClientException {
        return listQueues(new ListQueuesRequest());
    }

    public ListQueuesResult listQueues(ListQueuesRequest listQueuesRequest) throws AmazonServiceException, AmazonClientException {
        List<String> queueUrls = Lists.newArrayListWithCapacity(_queuesByUrl.size());
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

    public SendMessageBatchResult sendMessageBatch(SendMessageBatchRequest sendMessageBatchRequest) throws AmazonServiceException, AmazonClientException {
        DirectorySQSQueue queue = getQueueFromUrl(sendMessageBatchRequest.getQueueUrl(), false);
        //lists for reporting
        List<BatchResultErrorEntry> batchResultErrorEntries = new ArrayList<>();
        List<SendMessageBatchResultEntry> batchResultEntries = new ArrayList<>();
        //attempt to change the visibility on each
        for (SendMessageBatchRequestEntry batchRequestEntry : sendMessageBatchRequest.getEntries()) {
            try {
                final int invisibilityDelay = Objects.firstNonNull(batchRequestEntry.getDelaySeconds(), 0);//0 is amazon spec default
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

    //no
    public void addPermission(AddPermissionRequest addPermissionRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("not implemented");
    }

    //no
    public void removePermission(RemovePermissionRequest removePermissionRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("not implemented");
    }

    public void shutdown() {
        try {
            _queuesWatcher.close();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    //no
    public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest amazonWebServiceRequest) {
        throw new UnsupportedOperationException("not implemented");
    }

    //no
    public void setEndpoint(String s) throws IllegalArgumentException {
        throw new UnsupportedOperationException("not implemented");
    }

    //no
    public void setQueueAttributes(SetQueueAttributesRequest setQueueAttributesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public GetQueueAttributesResult getQueueAttributes(GetQueueAttributesRequest getQueueAttributesRequest) throws AmazonServiceException, AmazonClientException {
        DirectorySQSQueue queue = getQueueFromUrl(getQueueAttributesRequest.getQueueUrl(), false);
        Map<String, String> attributes = Maps.newHashMap();
        List<String> unsupported = Lists.newArrayList();

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

    //nothing
    @Override
    public void setRegion(Region region) throws IllegalArgumentException {
        throw new UnsupportedOperationException("not implemented");
    }
}

