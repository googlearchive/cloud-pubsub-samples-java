![status: inactive](https://img.shields.io/badge/status-inactive-red.svg)

This project is no longer actively developed or maintained.

For new work on this check out [App Engine Java 8 for Cloud Pub/Sub](https://github.com/GoogleCloudPlatform/java-docs-samples/tree/master/appengine-java8/pubsub).

# cloud-pubsub-samples-java

This sample is for using Cloud Pub/Sub on [App Engine
Standard](https://cloud.google.com/appengine/docs/java/). You might also be
interested in checking out the newer [Google Cloud Java Client
library](http://googlecloudplatform.github.io/google-cloud-java/),
which can be used with Java 8 on [App Engine
Flexible](https://cloud.google.com/appengine/docs/flexible/).

## appengine-push

Note: The push endpoints don't work with the App Engine Standard's local
devserver, since subscription endpoints must be over https, configured, and
publicly-accessible. The push notifications will go to an HTTPS URL on the App
Engine server even when you run this sample locally (see
[PubsubUtils.java][pubsubutils]). Therefore we recommend
you deploy and run the app on App Engine Standard. See [the
documentation][subscriber] for more details.

[pubsubutils]: src/main/java/com/google/cloud/pubsub/client/demos/appengine/util/PubsubUtils.java
[subscriber]: https://cloud.google.com/pubsub/docs/subscriber#create

## Register Your Application

- Go to https://cloud.google.com/console/project and create a new project.
- Go to the [App Engine Standard section][gae]
  of the console and enable your App Engine Standard app - it will have the same
  ID as the project. We will use this App Engine Standard application ID later,
  so make note of it.
- Enable the "Cloud Pub/Sub API" under "[API manager > Enable API][pubsub]"
- For local development, also [create and download a new service account
  key][service-account] for the App Engine Standard default service account.
  Save the JSON file in a secure location.

[gae]: https://console.cloud.google.com/appengine
[pubsub]: https://console.cloud.google.com/apis/api/pubsub.googleapis.com/overview
[service-account]: https://console.cloud.google.com/iam-admin/serviceaccounts/project

## Prerequisites

Install Java 7, Google Cloud SDK, and Maven 3.1.0 or higher. You may
need to [set your `JAVA_HOME`](https://g.co/cloud/bigtable/docs/installing-hbase-shell#setting_the_java_home_environment_variable).

## Configuration

- Edit src/main/webapp/WEB-INF/appengine-web.xml, and:
    - Set the
      `com.google.cloud.pubsub.client.demos.appengine.subscriptionUniqueToken`
      property with a unique alphanumeric combination of your choice.

- To enable logging of HTTP requests and responses (highly recommended
  when developing), please take a look at logging.properties. See also
  [the LogManager documentation][1].

## Running and Deploying Your Application from the Command Line

### To deploy your application to appspot.com:

You can deploy the application by running:

```
$ mvn clean appengine:update -DskipTests=true \
    -Dappengine.appId=<your-project-id> \
    -Dappengine.version=pubsub-sample
```

If this is the first time you have run "update" on the project, a
browser window will open prompting you to log in. Log in with the same
Google account the app is registered with.

Then access the following URL:

    https://pubsub-sample.<your-project-id>.appspot.com/

### To run your application locally on a development server:

(Again, note that the push endpoints don't work with the App Engine Standard's
local devserver, for reasons described above. We recommend you deploy and run
the app on App Engine Standard.)

- Set the following environment variable

  - `GOOGLE_APPLICATION_CREDENTIALS`: the file path to the downloaded JSON file.

Then start your server by running:

```
$ mvn clean appengine:devserver -DskipTests=true
```

[1]: http://docs.oracle.com/javase/6/docs/api/java/util/logging/LogManager.html
