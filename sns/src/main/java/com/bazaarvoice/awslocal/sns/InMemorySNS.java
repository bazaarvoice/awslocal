package com.bazaarvoice.awslocal.sns;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.sns.AbstractAmazonSNS;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.DeleteTopicRequest;
import com.amazonaws.services.sns.model.DeleteTopicResult;
import com.amazonaws.services.sns.model.InvalidParameterException;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicRequest;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicResult;
import com.amazonaws.services.sns.model.ListSubscriptionsRequest;
import com.amazonaws.services.sns.model.ListSubscriptionsResult;
import com.amazonaws.services.sns.model.ListTopicsRequest;
import com.amazonaws.services.sns.model.ListTopicsResult;
import com.amazonaws.services.sns.model.NotFoundException;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sns.model.Subscription;
import com.amazonaws.services.sns.model.Topic;
import com.amazonaws.services.sns.model.UnsubscribeRequest;
import com.amazonaws.services.sns.model.UnsubscribeResult;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;

public class InMemorySNS extends AbstractAmazonSNS {
    private static final Logger LOGGER = LoggerFactory.getLogger(InMemorySNS.class);
    private static final String BASE_ARN = "arn:aws:sns:local:user:";

    private Map<String, List<String>> _subscriptionsForTopic = new HashMap<>();
    private Map<String, Subscription> _subscriptionsByArn = new HashMap<>();
    private Random _random = new Random();

    private AmazonSQS _sqsClient;

    public InMemorySNS(AmazonSQS sqsClient, Subscription... subscriptions) {
        _sqsClient = sqsClient;
        for (Subscription subscription : subscriptions) {
            _subscriptionsByArn.put(subscription.getSubscriptionArn(), subscription);
            _subscriptionsForTopic.computeIfAbsent(subscription.getTopicArn(), key -> new ArrayList<>())
                    .add(subscription.getSubscriptionArn());
        }
    }

    @Override
    public PublishResult publish(PublishRequest publishRequest) throws AmazonClientException {
        String topicArn = publishRequest.getTopicArn();
        if (!_subscriptionsForTopic.containsKey(topicArn)) {
            throw new NotFoundException("no such topic " + topicArn);
        }
        _subscriptionsForTopic.get(topicArn).stream()
                .map(_subscriptionsByArn::get)
                .forEach(subscription -> {
                    String queueName = StringUtils.substringAfterLast(subscription.getEndpoint(), ":");
                    String queueUrl = _sqsClient.
                            getQueueUrl(new GetQueueUrlRequest().withQueueName(queueName)).
                            getQueueUrl();
                    _sqsClient.sendMessage(new SendMessageRequest().
                            withQueueUrl(queueUrl).
                            withMessageBody(publishRequest.getMessage()));
                });
        return new PublishResult();
    }

    @Override
    public SubscribeResult subscribe(SubscribeRequest subscribeRequest) throws AmazonClientException {
        final String protocol = subscribeRequest.getProtocol().toLowerCase();
        if (!protocol.equals("sqs")) {
            throw new InvalidParameterException("endpoint protocol " + protocol + " not supported");
        }
        final String topicArn = subscribeRequest.getTopicArn();
        if (!_subscriptionsForTopic.containsKey(topicArn)) {
            throw new InvalidParameterException("no such topic " + topicArn);
        }
        String subscriptionArn = topicArn + ":" + nextSubscriptionSuffix();
        while (_subscriptionsByArn.containsKey(subscriptionArn)) {
            // Avoid ID collision by generating a new suffix
            subscriptionArn = topicArn + ":" + nextSubscriptionSuffix();
        }
        _subscriptionsByArn.put(subscriptionArn, new Subscription().
                withTopicArn(topicArn).
                withProtocol(protocol).
                withSubscriptionArn(subscriptionArn).
                withEndpoint(subscribeRequest.getEndpoint()));
        _subscriptionsForTopic.get(topicArn).add(subscriptionArn);

        return new SubscribeResult().withSubscriptionArn(subscriptionArn);
    }

    /**
     * @return some 7-digit numeral
     */
    private int nextSubscriptionSuffix() {
        return 1000000 + _random.nextInt(9000000);
    }

    @Override
    public DeleteTopicResult deleteTopic(DeleteTopicRequest deleteTopicRequest) throws AmazonClientException {
        List<String> subscriptions = Optional.ofNullable(_subscriptionsForTopic.remove(deleteTopicRequest.getTopicArn()))
                .orElseGet(ArrayList::new);
        for (String subscription : subscriptions) {
            _subscriptionsByArn.remove(subscription);
        }
        return new DeleteTopicResult();
    }

    @Override
    public CreateTopicResult createTopic(CreateTopicRequest createTopicRequest) throws AmazonClientException {
        String topicArn = BASE_ARN + createTopicRequest.getName();
        CreateTopicResult result = new CreateTopicResult().withTopicArn(topicArn);
        _subscriptionsForTopic.putIfAbsent(topicArn, new ArrayList<>());
        return result;
    }

    @Override
    public UnsubscribeResult unsubscribe(UnsubscribeRequest unsubscribeRequest) throws AmazonClientException {
        if (!_subscriptionsByArn.containsKey(unsubscribeRequest.getSubscriptionArn()))
            throw new NotFoundException("no such subscription");
        Subscription removed = _subscriptionsByArn.remove(unsubscribeRequest.getSubscriptionArn());
        _subscriptionsForTopic.get(removed.getSubscriptionArn()).remove(removed.getSubscriptionArn());
        return new UnsubscribeResult();
    }

    @Override
    public ListSubscriptionsByTopicResult listSubscriptionsByTopic(ListSubscriptionsByTopicRequest listSubscriptionsByTopicRequest) throws AmazonClientException {
        return new ListSubscriptionsByTopicResult().
                withSubscriptions(_subscriptionsForTopic.get(listSubscriptionsByTopicRequest.getTopicArn()).stream()
                        .filter(_subscriptionsByArn.keySet()::contains)
                        .map(_subscriptionsByArn::get)
                        .collect(Collectors.toList()));
    }

    @Override
    public ListSubscriptionsResult listSubscriptions() throws AmazonClientException {
        return new ListSubscriptionsResult().withSubscriptions(_subscriptionsByArn.values());
    }

    @Override
    public ListSubscriptionsResult listSubscriptions(ListSubscriptionsRequest listSubscriptionsRequest) throws AmazonClientException {
        return listSubscriptions();
    }

    @Override
    public ListTopicsResult listTopics() throws AmazonClientException {
        return new ListTopicsResult().
                withTopics(_subscriptionsForTopic.keySet().stream()
                        .map(topicArn -> new Topic().withTopicArn(topicArn))
                        .collect(Collectors.toList()));
    }

    @Override
    public ListTopicsResult listTopics(ListTopicsRequest listTopicsRequest) throws AmazonClientException {
        return listTopics();
    }

}

