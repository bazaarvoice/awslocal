package com.bazaarvoice.awslocal.sqs;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.List;

@Test
public class TestSQSClientCooperation {

    private File _commonDirectory;
    private AmazonSQS _sqs1, _sqs2;

    @BeforeClass
    public void prepareClients()
    {
        try {
            _commonDirectory = TestUtils.createTempDirectory();
            _sqs1 = new DirectorySQS(_commonDirectory);
            _sqs2 = new DirectorySQS(_commonDirectory);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void willReceiveMessagesSentByAnother() {
        final String queueUrl = someNewQueue();
        final SendMessageResult sendMessageResult = _sqs1.sendMessage(new SendMessageRequest(queueUrl, someMessageBody()));

        final ReceiveMessageResult receiveMessageResult = _sqs2.receiveMessage(new ReceiveMessageRequest(queueUrl).withWaitTimeSeconds(20));
        Assert.assertEquals(receiveMessageResult.getMessages().size(), 1, "Expected one message");

        final Message message = receiveMessageResult.getMessages().get(0);
        Assert.assertEquals(message.getMessageId(), sendMessageResult.getMessageId());
        Assert.assertEquals(message.getMD5OfBody(), sendMessageResult.getMD5OfMessageBody());
    }

    public void client1CannotReceiveAfterClient2DoesWhenClient1Sends() {
        final String queueUrl = someNewQueue();
        final SendMessageResult sendMessageResult = _sqs1.sendMessage(new SendMessageRequest(queueUrl, someMessageBody()));

        final ReceiveMessageResult receiveMessageResult = _sqs2.receiveMessage(new ReceiveMessageRequest(queueUrl).withWaitTimeSeconds(20));
        Assert.assertEquals(receiveMessageResult.getMessages().size(), 1, "Expected one message");

        final Message message = receiveMessageResult.getMessages().get(0);
        Assert.assertEquals(message.getMessageId(), sendMessageResult.getMessageId());
        Assert.assertEquals(message.getMD5OfBody(), sendMessageResult.getMD5OfMessageBody());

        final ReceiveMessageResult receiveMessageResult2 = _sqs2.receiveMessage(new ReceiveMessageRequest(queueUrl));
        Assert.assertEquals(receiveMessageResult2.getMessages().size(), 0, "Expected no message");
    }

    public void client2CanReceiveAfterFailedReceive() {
        final String queueUrl = someNewQueue();
        final ReceiveMessageResult result1 = _sqs2.receiveMessage(new ReceiveMessageRequest(queueUrl).withWaitTimeSeconds(1).withMaxNumberOfMessages(1));
        Assert.assertEquals(result1.getMessages().size(), 0);

        final SendMessageResult sendResult = _sqs1.sendMessage(new SendMessageRequest(queueUrl, someMessageBody()));
        final ReceiveMessageResult result2 = _sqs2.receiveMessage(new ReceiveMessageRequest(queueUrl).withWaitTimeSeconds(10).withMaxNumberOfMessages(1));
        Assert.assertEquals(result2.getMessages().size(), 1);
    }

    public void client2CanReceiveTwice() {
        final String queueUrl = someNewQueue();
        final SendMessageResult sendResult1 = _sqs1.sendMessage(new SendMessageRequest(queueUrl, someMessageBody()));
        final ReceiveMessageResult result1 = _sqs2.receiveMessage(new ReceiveMessageRequest(queueUrl).
                withWaitTimeSeconds(10).
                withVisibilityTimeout(30).
                withMaxNumberOfMessages(1));
        Assert.assertEquals(result1.getMessages().size(), 1, "first receive failed");

        final SendMessageResult sendResult2 = _sqs1.sendMessage(new SendMessageRequest(queueUrl, someMessageBody()));
        final ReceiveMessageResult result2 = _sqs2.receiveMessage(new ReceiveMessageRequest(queueUrl).withWaitTimeSeconds(20).withMaxNumberOfMessages(1));
        Assert.assertEquals(result2.getMessages().size(), 1, "second receive failed");
    }

    public void client2CanReceiveTwiceAfterInitialEmpty() {
        final String queueUrl = someNewQueue();
        final ReceiveMessageResult result1 = _sqs2.receiveMessage(new ReceiveMessageRequest(queueUrl).withWaitTimeSeconds(1).withMaxNumberOfMessages(1));
        Assert.assertEquals(result1.getMessages().size(), 0);

        final SendMessageResult sendResult1 = _sqs1.sendMessage(new SendMessageRequest(queueUrl, someMessageBody()));
        final ReceiveMessageResult result2 = _sqs2.receiveMessage(new ReceiveMessageRequest(queueUrl).
                withWaitTimeSeconds(10).
                withMaxNumberOfMessages(1).
                withVisibilityTimeout(60));
        Assert.assertEquals(result2.getMessages().size(), 1, "first receive failed");

        final SendMessageResult sendResult2 = _sqs1.sendMessage(new SendMessageRequest(queueUrl, someMessageBody()));
        final ReceiveMessageResult result3 = _sqs2.receiveMessage(new ReceiveMessageRequest(queueUrl).
                withWaitTimeSeconds(20).
                withMaxNumberOfMessages(1));
        Assert.assertEquals(result3.getMessages().size(), 1, "second receive failed");
    }

    public void fiveMessagesToClient2FiveToClient1() {
        final String queueUrl = someNewQueue();

        final List<SendMessageResult> sendResults = Lists.newArrayListWithCapacity(10);

        for (int i = 0; i < 10; i++ ) {
            sendResults.add(_sqs1.sendMessage(new SendMessageRequest(queueUrl, someMessageBody())));
        }

        HashSet<String> sentIds = new HashSet<>(10);
        HashSet<String> sentBodiesMD5 = new HashSet<>(10);

        for (SendMessageResult sendMessageResult : sendResults) {
            sentIds.add(sendMessageResult.getMessageId());
            sentBodiesMD5.add(sendMessageResult.getMD5OfMessageBody());
        }
        final ReceiveMessageResult receiveMessageResult1 = _sqs2.receiveMessage(new ReceiveMessageRequest(queueUrl)
                .withWaitTimeSeconds(20)
                .withMaxNumberOfMessages(5));

        Assert.assertEquals(receiveMessageResult1.getMessages().size(), 5, "c1 did not get 5 messages");

        final ReceiveMessageResult receiveMessageResult2 = _sqs2.receiveMessage(new ReceiveMessageRequest(queueUrl).
                withWaitTimeSeconds(20).
                withMaxNumberOfMessages(5));

        Assert.assertEquals(receiveMessageResult2.getMessages().size(), 5, "c2 did not get 5 messages");
        for (Message message : Iterables.concat(receiveMessageResult1.getMessages(), receiveMessageResult2.getMessages())) {
            Assert.assertTrue(sentIds.contains(message.getMessageId()));
            sentIds.remove(message.getMessageId());
            Assert.assertTrue(sentBodiesMD5.contains(message.getMD5OfBody()));
        }
    }

    public void client1GetsFromBoth() {
        final String queueUrl = someNewQueue();
        final SendMessageResult sendResult1 = _sqs1.sendMessage(new SendMessageRequest(queueUrl, someMessageBody()));
        final SendMessageResult sendResult2 = _sqs2.sendMessage(new SendMessageRequest(queueUrl, someMessageBody()));

        final ReceiveMessageResult receiveMessageResult1 = _sqs1.receiveMessage(new ReceiveMessageRequest(queueUrl).
                withMaxNumberOfMessages(1).
                withWaitTimeSeconds(20));

        Assert.assertEquals(receiveMessageResult1.getMessages().size(), 1);

        final ReceiveMessageResult receiveMessageResult2 = _sqs1.receiveMessage(new ReceiveMessageRequest(queueUrl).
                withWaitTimeSeconds(20));

        Assert.assertEquals(receiveMessageResult2.getMessages().size(), 1);
    }

    private String someNewQueue() {
        final String queueName = someQueueName();
        final CreateQueueResult queue = _sqs1.createQueue(new CreateQueueRequest(queueName));
        return queue.getQueueUrl();
    }

    private String someQueueName() {
        return RandomStringUtils.randomAlphanumeric(32);
    }

    private String someMessageBody() {
        return RandomStringUtils.random(1024);
    }
}

