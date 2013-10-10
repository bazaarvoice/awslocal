package com.bazaarvoice.awslocal.sns;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.AddPermissionRequest;
import com.amazonaws.services.sns.model.ConfirmSubscriptionRequest;
import com.amazonaws.services.sns.model.ConfirmSubscriptionResult;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.DeleteTopicRequest;
import com.amazonaws.services.sns.model.GetSubscriptionAttributesRequest;
import com.amazonaws.services.sns.model.GetSubscriptionAttributesResult;
import com.amazonaws.services.sns.model.GetTopicAttributesRequest;
import com.amazonaws.services.sns.model.GetTopicAttributesResult;
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
import com.amazonaws.services.sns.model.RemovePermissionRequest;
import com.amazonaws.services.sns.model.SetSubscriptionAttributesRequest;
import com.amazonaws.services.sns.model.SetTopicAttributesRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sns.model.Subscription;
import com.amazonaws.services.sns.model.Topic;
import com.amazonaws.services.sns.model.UnsubscribeRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InMemorySNS implements AmazonSNS {
    private static final Logger LOGGER = LoggerFactory.getLogger(InMemorySNS.class);
    private static final Function<String,Topic> NEW_TOPIC = new Function<String, Topic>() {
        @Override
        public Topic apply(String s) {
            return new Topic().withTopicArn(s);
        }
    };
    private static final String BASE_ARN = "arn:aws:sns:local:user:";

    private Map<String, List<String>> _subscriptionsForTopic = Maps.newHashMap();
    private Map<String, Subscription> _subscriptionsByArn = Maps.newHashMap();

    private AmazonSQS _sqsClient;

    public InMemorySNS(AmazonSQS sqsClient, Subscription... subscriptions) {
        _sqsClient = sqsClient;
        for (Subscription subscription : subscriptions) {
            _subscriptionsByArn.put(subscription.getSubscriptionArn(), subscription);
            if (!_subscriptionsForTopic.containsKey(subscription.getTopicArn())) {
                _subscriptionsForTopic.put(subscription.getTopicArn(), new ArrayList<String>());
            }
            _subscriptionsForTopic.get(subscription.getTopicArn()).add(subscription.getSubscriptionArn());
        }
    }

    public PublishResult publish(PublishRequest publishRequest) throws AmazonServiceException, AmazonClientException {
        String topicArn = publishRequest.getTopicArn();
        if (!_subscriptionsForTopic.containsKey(topicArn)) {
            throw new NotFoundException("no such topic " + topicArn);
        }
        List<Subscription> topicSubscriptions = FluentIterable.
                from(_subscriptionsForTopic.get(topicArn)).
                transform(Functions.forMap(_subscriptionsByArn)).
                toList();
        for (Subscription subscription : topicSubscriptions) {
            String queueName = getLast(subscription.getEndpoint().split(":"));
            String queueUrl = _sqsClient.
                    getQueueUrl(new GetQueueUrlRequest().withQueueName(queueName)).
                    getQueueUrl();
            _sqsClient.sendMessage(new SendMessageRequest().
                    withQueueUrl(queueUrl).
                    withMessageBody(publishRequest.getMessage()));
        }
        return new PublishResult();
    }

    public SubscribeResult subscribe(SubscribeRequest subscribeRequest) throws AmazonServiceException, AmazonClientException {
        final String protocol = subscribeRequest.getProtocol().toLowerCase();
        if (!protocol.equals("sqs")) {
            throw new InvalidParameterException("endpoint protocol " + protocol + " not supported");
        }
        final String topicArn = subscribeRequest.getTopicArn();
        if (!_subscriptionsForTopic.containsKey(topicArn)) {
            throw new InvalidParameterException("no such topic " + topicArn);
        }
        String subscriptionArn = topicArn + ":" + RandomStringUtils.randomNumeric(7);
        if (!_subscriptionsByArn.containsKey(subscriptionArn)) {
            _subscriptionsByArn.put(subscriptionArn, new Subscription().
                    withTopicArn(topicArn).
                    withProtocol(protocol).
                    withSubscriptionArn(subscriptionArn).
                    withEndpoint(subscribeRequest.getEndpoint()));
            _subscriptionsForTopic.get(topicArn).add(subscriptionArn);
        }

        return new SubscribeResult().withSubscriptionArn(subscriptionArn);
    }

    public void deleteTopic(DeleteTopicRequest deleteTopicRequest) throws AmazonServiceException, AmazonClientException {
        List<String> subscriptions = Objects.firstNonNull(
                _subscriptionsForTopic.remove(deleteTopicRequest.getTopicArn()),
                new ArrayList<String>());
        for (String subscription : subscriptions) {
            _subscriptionsByArn.remove(subscription);
        }
    }

    public CreateTopicResult createTopic(CreateTopicRequest createTopicRequest) throws AmazonServiceException, AmazonClientException {
        String topicArn = BASE_ARN + createTopicRequest.getName();
        CreateTopicResult result = new CreateTopicResult().withTopicArn(topicArn);
        if (!_subscriptionsForTopic.containsKey(topicArn)) {
            _subscriptionsForTopic.put(topicArn, new ArrayList<String>());
        }
        return result;
    }

    public void unsubscribe(UnsubscribeRequest unsubscribeRequest) throws AmazonServiceException, AmazonClientException {
        if (!_subscriptionsByArn.containsKey(unsubscribeRequest.getSubscriptionArn()))
            throw new NotFoundException("no such subscription");
        Subscription removed = _subscriptionsByArn.remove(unsubscribeRequest.getSubscriptionArn());
        _subscriptionsForTopic.get(removed.getSubscriptionArn()).remove(removed.getSubscriptionArn());
    }

    public ListSubscriptionsByTopicResult listSubscriptionsByTopic(ListSubscriptionsByTopicRequest listSubscriptionsByTopicRequest) throws AmazonServiceException, AmazonClientException {
        return new ListSubscriptionsByTopicResult().
                withSubscriptions(FluentIterable.
                        from(_subscriptionsForTopic.get(listSubscriptionsByTopicRequest.getTopicArn())).
                        filter(Predicates.in(_subscriptionsByArn.keySet())).
                        transform(Functions.forMap(_subscriptionsByArn)).
                        toList());
    }

    public ListSubscriptionsResult listSubscriptions() throws AmazonServiceException, AmazonClientException {
        return new ListSubscriptionsResult().withSubscriptions(_subscriptionsByArn.values());
    }

    public ListSubscriptionsResult listSubscriptions(ListSubscriptionsRequest listSubscriptionsRequest) throws AmazonServiceException, AmazonClientException {
        return listSubscriptions();
    }

    public ListTopicsResult listTopics() throws AmazonServiceException, AmazonClientException {
        return new ListTopicsResult().
                withTopics(FluentIterable.
                        from(_subscriptionsForTopic.keySet()).
                        transform(NEW_TOPIC).
                        toList());
    }

    public ListTopicsResult listTopics(ListTopicsRequest listTopicsRequest) throws AmazonServiceException, AmazonClientException {
        return listTopics();
    }

    public ConfirmSubscriptionResult confirmSubscription(ConfirmSubscriptionRequest confirmSubscriptionRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("not implemented");
    }

    public void shutdown() {
        throw new UnsupportedOperationException("not implemented");
    }

    public void removePermission(RemovePermissionRequest removePermissionRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("not implemented");
    }

    public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
        throw new UnsupportedOperationException("not implemented");
    }

    public void setEndpoint(String endpoint) throws IllegalArgumentException {
        throw new UnsupportedOperationException("not implemented");
    }

    public void setRegion(Region region) throws IllegalArgumentException {
        throw new UnsupportedOperationException("not implemented");
    }

    public void setTopicAttributes(SetTopicAttributesRequest setTopicAttributesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("not implemented");
    }

    public GetTopicAttributesResult getTopicAttributes(GetTopicAttributesRequest getTopicAttributesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("not implemented");
    }

    public void setSubscriptionAttributes(SetSubscriptionAttributesRequest setSubscriptionAttributesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("not implemented");
    }

    public void addPermission(AddPermissionRequest addPermissionRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("not implemented");
    }

    public GetSubscriptionAttributesResult getSubscriptionAttributes(GetSubscriptionAttributesRequest getSubscriptionAttributesRequest) throws AmazonServiceException, AmazonClientException {
        throw new UnsupportedOperationException("not implemented");
    }

    private static <T> T getLast(T[] array) {
        return array.length > 0 ? array[array.length - 1] : null;
    }

}

