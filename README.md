awslocal
========

Implements some of the AWS Java SDK interfaces so that they can work entirely on the local machine.

Which interfaces?
-----------------
Currently, this implements:
- SQS queues, by representing queues as directories and messages as files.
- SNS, but only with the SQS protocol (it represents the topics in memory, and it takes an AmazonSQS implementation to send to SQS protocol subscriptions).

Using it
--------
This library is built in Java 7.
- SQS: 

  ```java
  AmazonSQS sqsClient = new DirectorySQS("<base directory for queues>")
  ```

  Then just use sqsClient just as the SDK specifies - create queues and send messages!
  
- SNS:

  ```java
  AmazonSNS snsClient = new InMemorySNS(<something implementing AmazonSQS>)
  ```

  Again, use it as if it were SNS; subscribe queues to topics, and broadcast to those topics! Note that topics only exist for a specific InMemorySNS.

  ```java
  AmazonSQS sqs = new DirectorySQS(directory);
  AmazonSNS sns = new InMemorySNS(sqs);

  String topicName = "topicName";
  String queueName = "queueName";
  CreateTopicResult topicResult = sns.createTopic(new CreateTopicRequest(topicName));
  CreateQueueResult queueResult = sqs.createQueue(new CreateQueueRequest(queueName));
  GetQueueAttributesResult queueAttributesResult = sqs.getQueueAttributes(new GetQueueAttributesRequest(queueResult.getQueueUrl()).withAttributeNames("QueueArn"));
  sns.subscribe(new SubscribeRequest(topicResult.getTopicArn(), "sqs", queueAttributesResult.getAttributes().get("QueueArn")));
  ```
See the tests for further examples of use.

Further development
--------------------
These implementations have been tested fairly thoroughly, so you should be able to rely on them for testing projects that use SNS and SQS.
We (Robby and Aidan) haven't needed any other interfaces implemented yet, so we don't have plans to add more stuff.
However, if you find any bugs, please do open a ticket, and we'll see what we can do!
If you want to add implementations for further interfaces, send us a pull request and we'll take a look.

Licensing
---------
See the LICENSE file.
