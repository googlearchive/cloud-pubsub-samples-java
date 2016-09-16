# cloud-pubsub-samples-java

## cmdline-pull

This is a command line sample application using the Cloud Pub/Sub
API. You can do the following things with this command line tool:

- Connect to an IRC channel and publish IRC messages.
- Pull messages from a subscription and print those messages.

## Prerequisites

Install Maven and Java7.

## Register your application

- If you don't have a project, go to [Google Developers Console][1]
  and create a new project.

- Enable the "Google Cloud Pub/Sub" API under "APIs & auth > APIs."

- Go to "Credentials" and create a new Service Account,
  then download a new JSON file.

- Set the following environment variables.

  - GOOGLE_APPLICATION_CREDENTIALS: the file path to the downloaded JSON file.

## Build the application
- Set the following environment variable.
  - TEST_PROJECT_ID: the ID of your Google Cloud project.
  - note: the above environment variable is only required during the build phase.

```
$ mvn package
```

## Run the application

```
$ bin/pubsub-sample.sh
```
or

```
C:\ > bin\pubsub-sample.bat
```

This will give you a help message. Here is an example session with
this command.

```
# create a new topic "test" on MYPROJ
$ bin/pubsub-sample.sh MYPROJ create_topic test

# list the current topics
$ bin/pubsub-sample.sh MYPROJ list_topics

# create a new subscription "sub" on the "test" topic
$ bin/pubsub-sample.sh MYPROJ create_subscription sub test

# publish a message "hello" to the "test" topic
$ bin/pubsub-sample.sh MYPROJ publish_message test hello

# connect to the Wikipedia recent change channel
$ bin/pubsub-sample.sh \
  MYPROJ \
  connect_irc \
  test \
  irc.wikimedia.org \
  "#en.wikipedia"

# fetch messages from the subscription "sub"
$ bin/pubsub-sample.sh MYPROJ pull_messages sub
```

Please enjoy!


[1]: https://console.developers.google.com/project
