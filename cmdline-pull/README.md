cloud-pubsub-samples-java
=========================

cmdline-pull
------------

This is a command line sample application using the Cloud Pub/Sub
API. You can do the following things with this command line tool:

* Connect to an IRC channel and publish IRC massages.
* Pull messages from a subscription and print those messages.

= Prerequisites

- Install Maven and Java7.

= Register your application

1. If you don't have a project, go to [Google Developers Console][1]
   and create a new project.

2. Enable the "Google Cloud Pub/Sub" API under "APIs & auth > APIs."

3. Go to "Credentials" and create a new Client ID by selecting
   "Installed application" and "Other". Then click the "Download JSON"
   button and save it as "src/main/resources/client_secrets.json".

= Build the application

  $ mvn package

= Run the application

  $ bin/pubsub-sample.sh
  or
  C:\ > bin\pubsub-sample.bat

  This will give you a help message. Here is an example session with
  this command.

  # create a new topic "test" on MYPROJ
  $ bin/pubsub-sample.sh MYPROJ create_topic test

  # list the current topics
  $ bin/pubsub-sample.sh MYPROJ list_topics

  # create a new subscription "sub" on the "test" topic
  $ bin/pubsub-sample.sh MYPROJ create_subscription sub test

  # connect to the Wikipedia recent change channel
  $ bin/pubsub-sample.sh \
    MYPROJ \
    connect_irc \
    test \
    irc.wikimedia.org \
    "#en.wikipedia"

  # fetch messages from the subscription "sub"
  $ bin/pubsub-sample.sh MYPROJ pull_messages sub

Enjoy!


[1]: https://console.developers.google.com/project
