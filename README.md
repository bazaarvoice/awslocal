awslocal
========

Implements some of the AWS Java SDK interfaces so that they can work entirely on the local machine.

Which interfaces?
-----------------
Currently, this implements:
- SQS queues, by representing queues as directories and messages as files.
- SNS, but only with the SQS protocol (it represents the topics in memory, and it takes an AmazonSQS implementation to send to SQS protocol subscriptions).


Further development
--------------------
These implementations have been tested fairly thoroughly, so you should be able to rely on them for testing projects that use SNS and SQS.
We (Robby and Aidan) haven't needed any other interfaces implemented yet, so we don't have plans to add more stuff.
However, if you find any bugs, please do open a ticket, and we'll see what we can do!
If you want to add implementations for further interfaces, send us a pull request and we'll take a look.

Licensing
---------
See the LICENSE file.