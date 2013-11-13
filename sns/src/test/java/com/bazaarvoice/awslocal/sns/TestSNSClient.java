package com.bazaarvoice.awslocal.sns;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicRequest;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.Subscription;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.bazaarvoice.awslocal.sqs.DirectorySQS;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

@Test
public class TestSNSClient {

    private AmazonSQS _amazonSQS1;
    private AmazonSQS _amazonSQS2;
    private File _baseDir;

    @BeforeClass
    public void prepareClient()
    {
        try {
            _baseDir = TestUtils.createTempDirectory();
            _amazonSQS1 = new DirectorySQS(_baseDir);
            _amazonSQS2 = new DirectorySQS(_baseDir);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void publishAndReceive() {
        final String queueName = someQueueName();
        final String queueUrl = someNewQueue(queueName);
        final String topicName = "publishAndReceive";
        final String message = "hi from " + topicName;

        AmazonSNS amazonSNS = new InMemorySNS(_amazonSQS1,
                new Subscription().
                        withTopicArn(makeTopicArn(topicName)).
                        withProtocol("sqs").
                        withSubscriptionArn(makeSomeSubArn(topicName)).
                        withEndpoint(makeQueueArn(queueName)));

        Assert.assertEquals(amazonSNS.listTopics().getTopics().size(), 1);
        Assert.assertEquals(amazonSNS.listSubscriptions().getSubscriptions().size(), 1);
        Assert.assertEquals(amazonSNS.
                listSubscriptionsByTopic(new ListSubscriptionsByTopicRequest(makeTopicArn(topicName)))
                .getSubscriptions()
                .size(),
                1);


        amazonSNS.publish(new PublishRequest(makeTopicArn(topicName), message));
        ReceiveMessageResult result = _amazonSQS1.receiveMessage(new ReceiveMessageRequest(queueUrl));
        Assert.assertEquals(result.getMessages().size(), 1);
        Assert.assertEquals(result.getMessages().get(0).getBody(), message);
    }

    public void publishAndReceiveSeparateSQSClients() {
        final String queueName = someQueueName();
        final String queueUrl = someNewQueue(queueName);
        final String topicName = "publishAndReceiveSeparateSQSClients";
        final String message = "hi from " + topicName;

        AmazonSNS amazonSNS = new InMemorySNS(_amazonSQS1,
                new Subscription().
                        withTopicArn(makeTopicArn(topicName)).
                        withProtocol("sqs").
                        withSubscriptionArn(makeSomeSubArn(topicName)).
                        withEndpoint(makeQueueArn(queueName)));

        amazonSNS.publish(new PublishRequest(makeTopicArn(topicName), message));

        ReceiveMessageResult result = _amazonSQS2.receiveMessage(new ReceiveMessageRequest(queueUrl).
                withWaitTimeSeconds(15));
        Assert.assertEquals(result.getMessages().size(), 1);
        Assert.assertEquals(result.getMessages().get(0).getBody(), message);
    }

    public void manualSubscriptionSetup() {
        final String queueName = someQueueName();
        final String queueUrl = someNewQueue(queueName);
        final String topicName = "manualSubscriptionSetup";
        final String message = "hi from " + topicName;

        AmazonSNS amazonSNS = new InMemorySNS(_amazonSQS1);

        amazonSNS.createTopic(new CreateTopicRequest(topicName));
        Assert.assertEquals(amazonSNS.listTopics().getTopics().size(), 1);
        Assert.assertEquals(amazonSNS.listTopics().getTopics().get(0).getTopicArn(), makeTopicArn(topicName));

        //make sure create is idempotent
        amazonSNS.createTopic(new CreateTopicRequest(topicName));
        Assert.assertEquals(amazonSNS.listTopics().getTopics().size(), 1, "existing topic duplicated?");
        Assert.assertEquals(amazonSNS.listTopics().getTopics().get(0).getTopicArn(), makeTopicArn(topicName));

        //subscribe.
        amazonSNS.subscribe(new SubscribeRequest().
                withEndpoint(makeQueueArn(queueName)).
                withProtocol("sqs").
                withTopicArn(makeTopicArn(topicName)));

        Assert.assertEquals(amazonSNS.listSubscriptions().getSubscriptions().size(), 1);
        Assert.assertEquals(amazonSNS.listSubscriptions().getSubscriptions().get(0).getTopicArn(), makeTopicArn(topicName));
        Assert.assertEquals(amazonSNS.
                listSubscriptionsByTopic(new ListSubscriptionsByTopicRequest(makeTopicArn(topicName)))
                .getSubscriptions()
                .size(),
                1);

        amazonSNS.publish(new PublishRequest(makeTopicArn(topicName), message));

        ReceiveMessageResult result = _amazonSQS1.receiveMessage(new ReceiveMessageRequest(queueUrl));
        Assert.assertEquals(result.getMessages().size(), 1);
        Assert.assertEquals(result.getMessages().get(0).getBody(), message);
    }

    private String makeQueueArn(String queueName) {
        return "arn:aws:sqs:local:user:" + queueName;
    }

    private String makeTopicArn(String name) {
        return "arn:aws:sns:local:user:" + name;
    }

    private String makeSomeSubArn(String topicName) {
        return makeTopicArn(topicName) + ":" + RandomStringUtils.randomNumeric(7);
    }

    private String someNewQueue(String queueName) {
        final CreateQueueResult queue = _amazonSQS1.createQueue(new CreateQueueRequest(queueName));
        return queue.getQueueUrl();
    }

    private String someQueueName() {
        return RandomStringUtils.randomAlphanumeric(32);
    }
}

