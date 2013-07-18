package com.bazaarvoice.awslocal.sqs;


import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.QueueNameExistsException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.google.common.base.Throwables;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;

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
        _amazonSQS.deleteQueue(new DeleteQueueRequest(new File(createTempDirectory(), someQueueName()).toURI().toString()));
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
        return new DirectorySQS(createTempDirectory());
    }

    public static File createTempDirectory() {
        try {
            final File directory = Files.createTempDirectory("sqs").toFile();
            directory.deleteOnExit(); // not sure if this works
            return directory;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

}
