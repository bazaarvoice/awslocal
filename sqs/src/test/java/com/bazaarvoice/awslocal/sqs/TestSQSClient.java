package com.bazaarvoice.awslocal.sqs;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.QueueNameExistsException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Test
public class TestSQSClient {

    private AmazonSQS _amazonSQS;

    @BeforeClass
    public void prepareClient()
    {
        try {
            _amazonSQS = createSQSClient();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }


    @Test(expectedExceptions = QueueDoesNotExistException.class)
    public void failsOnNonExistentQueue() {
        _amazonSQS.getQueueUrl(new GetQueueUrlRequest(someQueueName()));
    }

    public void canCreateQueue() {
        final String queueName = someQueueName();

        final CreateQueueResult queue = _amazonSQS.createQueue(new CreateQueueRequest(queueName));
        Assert.assertNotNull(queue.getQueueUrl(), "Queue URL should be present");

        final GetQueueUrlResult result = _amazonSQS.getQueueUrl(new GetQueueUrlRequest(queueName));
        Assert.assertEquals(result.getQueueUrl(), queue.getQueueUrl());
    }

    @Test(expectedExceptions = QueueNameExistsException.class)
    public void cannotRecreateQueue() {
        final String queueName = someQueueName();

        _amazonSQS.createQueue(new CreateQueueRequest(queueName));

        // this should fail
        _amazonSQS.createQueue(new CreateQueueRequest(queueName));
    }

    @Test(expectedExceptions = QueueDoesNotExistException.class)
    public void cannotDeleteNonExistentQueue()
            throws IOException {
        _amazonSQS.deleteQueue(new DeleteQueueRequest(new File(TestUtils.createTempDirectory(), someQueueName()).toURI().toString()));
    }

    @Test(expectedExceptions = AmazonServiceException.class)
    public void deletedQueueNoLongerExists() {
        final String queueUrl = someNewQueue();

        _amazonSQS.deleteQueue(new DeleteQueueRequest(queueUrl));

        // this should fail
        _amazonSQS.receiveMessage(new ReceiveMessageRequest(queueUrl));
    }

    public void willReceiveNoMessagesOnNewQueue() {
        final String queueUrl = someNewQueue();

        verifyReceiveNone(queueUrl);
    }

    private void verifyReceiveNone(String queueUrl) {
        final ReceiveMessageResult result = _amazonSQS.receiveMessage(new ReceiveMessageRequest(queueUrl));

        Assert.assertEquals(result.getMessages(), Collections.emptyList());
    }

    public void willSendAndReceiveMessage() {
        final String queueUrl = someNewQueue();

        final String messageBody = someMessageBody();

        final SendMessageResult sendResult = _amazonSQS.sendMessage(new SendMessageRequest(queueUrl, messageBody));
        Assert.assertNotNull(sendResult.getMD5OfMessageBody());

        final ReceiveMessageResult receiveResult = _amazonSQS.receiveMessage(new ReceiveMessageRequest(queueUrl));

        Assert.assertEquals(receiveResult.getMessages().size(), 1, "Expected one message");

        final Message message = receiveResult.getMessages().get(0);

        Assert.assertEquals(message.getMessageId(), sendResult.getMessageId());
        Assert.assertEquals(message.getBody(), messageBody);
        Assert.assertEquals(message.getMD5OfBody(), sendResult.getMD5OfMessageBody());
    }

    public void waitTimeRequirement() {
        final String queueUrl = someNewQueue();
        int count = 0;
        try {
            final ReceiveMessageResult receiveResult = _amazonSQS.receiveMessage(new ReceiveMessageRequest(queueUrl).withWaitTimeSeconds(21));
        } catch (AmazonServiceException e) {
            Assert.assertTrue(e.getMessage().contains("21"));
            count++;
        }
        try {
            final ReceiveMessageResult receiveResult = _amazonSQS.receiveMessage(new ReceiveMessageRequest(queueUrl).withWaitTimeSeconds(-1));
        } catch (AmazonServiceException e) {
            Assert.assertTrue(e.getMessage().contains("-1"));
            count++;
        }
        Assert.assertEquals(count, 2);
    }

    public void willSendAndReceiveMultipleMessages() {
        final String queueUrl = someNewQueue();

        final String messageBody = someMessageBody();
        final String anotherBody = someMessageBody();

        final SendMessageResult sendResult = _amazonSQS.sendMessage(new SendMessageRequest(queueUrl, messageBody));
        final SendMessageResult sendResult2 = _amazonSQS.sendMessage(new SendMessageRequest(queueUrl, anotherBody));

        final ReceiveMessageResult receiveResult = _amazonSQS.receiveMessage(new ReceiveMessageRequest(queueUrl));

        Assert.assertEquals(receiveResult.getMessages().size(), 2, "Expected two messages");

        final Message message = receiveResult.getMessages().get(0);

        Assert.assertEquals(message.getMessageId(), sendResult.getMessageId());
        Assert.assertEquals(message.getBody(), messageBody);
        Assert.assertEquals(message.getMD5OfBody(), sendResult.getMD5OfMessageBody());

        final Message message2 = receiveResult.getMessages().get(1);

        Assert.assertEquals(message2.getMessageId(), sendResult2.getMessageId());
        Assert.assertEquals(message2.getBody(), anotherBody);
        Assert.assertEquals(message2.getMD5OfBody(), sendResult2.getMD5OfMessageBody());
    }

    public void willReceiveMessageAfterTimeout()
            throws InterruptedException {
        final String queueUrl = someNewQueue();

        final String messageBody = someMessageBody();

        final SendMessageResult sendResult = _amazonSQS.sendMessage(new SendMessageRequest(queueUrl, messageBody));
        Assert.assertNotNull(sendResult.getMD5OfMessageBody());

        verifyReceiveEmail(sendResult.getMessageId(), queueUrl, 1);
        sleep(1);

        verifyReceiveEmail(sendResult.getMessageId(), queueUrl, 2);
        sleep(1);

        verifyReceiveNone(queueUrl);
        sleep(1);

        final String receiptHandle = verifyReceiveEmail(sendResult.getMessageId(), queueUrl, 1);
        _amazonSQS.changeMessageVisibility(new ChangeMessageVisibilityRequest(queueUrl, receiptHandle, 3));
        sleep(2);

        verifyReceiveNone(queueUrl);
        sleep(1);

        verifyReceiveEmail(sendResult.getMessageId(), queueUrl, null);
    }

    public void willNotReceiveDeletedMessages()
            throws InterruptedException {
        final String queueUrl = someNewQueue();

        final String messageBody = someMessageBody();

        final SendMessageResult sendResult = _amazonSQS.sendMessage(new SendMessageRequest(queueUrl, messageBody));
        Assert.assertNotNull(sendResult.getMD5OfMessageBody());

        final String receiptHandle = verifyReceiveEmail(sendResult.getMessageId(), queueUrl, 1);
        _amazonSQS.deleteMessage(new DeleteMessageRequest(queueUrl, receiptHandle));

        sleep(1);

        verifyReceiveNone(queueUrl);
    }

    public void willNotReceiveAfterPurged()
            throws InterruptedException {
        final String queueUrl = someNewQueue();

        for (int i = 0; i < 3; i++) {
            _amazonSQS.sendMessage(new SendMessageRequest(queueUrl, someMessageBody()));
        }

        _amazonSQS.purgeQueue(new PurgeQueueRequest(queueUrl));

        sleep(1);

        verifyReceiveNone(queueUrl);
    }

    public void getQueueArnFromAttributes() {
        String queueName = someQueueName();
        CreateQueueResult createQueueResult = _amazonSQS.createQueue(new CreateQueueRequest(queueName));
        String queueUrl = createQueueResult.getQueueUrl();

        List<String> requestedAttributes = ImmutableList.of("QueueArn");
        GetQueueAttributesResult getQueueAttributesResult = _amazonSQS.getQueueAttributes(new GetQueueAttributesRequest()
                .withQueueUrl(queueUrl)
                .withAttributeNames(requestedAttributes));
        Map<String, String> resultAttributes = getQueueAttributesResult.getAttributes();
        String queueArn = resultAttributes.get("QueueArn");
        String queueNameFromArn = queueArn.substring(queueArn.lastIndexOf(":") + 1);

        Assert.assertEquals(queueNameFromArn, queueName);
    }

    private void sleep(int seconds)
            throws InterruptedException {
        Thread.sleep(seconds * 1000);
    }

    private String verifyReceiveEmail(String expectedMessageId, String queueUrl, Integer visibilityTimeout) {
        final ReceiveMessageResult receiveResult = _amazonSQS.receiveMessage(new ReceiveMessageRequest(queueUrl).withVisibilityTimeout(visibilityTimeout));

        Assert.assertEquals(receiveResult.getMessages().size(), 1, "Expected one message");

        final Message message = receiveResult.getMessages().get(0);
        Assert.assertEquals(message.getMessageId(), expectedMessageId);
        return message.getReceiptHandle();
    }

    private String someNewQueue() {
        final String queueName = someQueueName();
        final CreateQueueResult queue = _amazonSQS.createQueue(new CreateQueueRequest(queueName));
        return queue.getQueueUrl();
    }

    private String someQueueName() {
        return RandomStringUtils.randomAlphanumeric(32);
    }

    private String someMessageBody() {
        return RandomStringUtils.random(1024);
    }

    private DirectorySQS createSQSClient()
            throws IOException {
        return new DirectorySQS(TestUtils.createTempDirectory());
    }
}

