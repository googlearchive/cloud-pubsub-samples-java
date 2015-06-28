# cloud-pubsub-samples-java

## appengine-push

Note: The push endpoints don't work with the App Engine's local
devserver. The push notifications will go to an HTTP URL on the App
Engine server even when you run this sample locally. So we recommend
you deploy and run the app on App Engine.
TODO(tmatsuo): Better implementation for devserver.

## Register Your Application

- Go to https://cloud.google.com/console/project and create a new
  project. This will automatically enable an App Engine application
  with the same ID as the project. We will use this App Engine
  application ID later, so make note of the application ID.
- Enable the "Google Cloud Pub/Sub" API under "APIs & auth > APIs."
- For local development, also create a new client ID of type Service
  account. Save the JSON file in a secure location.

## Prerequisites

Install Java 7, Google Cloud SDK, and Maven 3.1.0 or higher. You may
need to set your JAVA_HOME.

## Configuration

- Edit src/main/webapp/WEB-INF/appengine-web.xml, and:
    - Set the
      com.google.cloud.pubsub.client.demos.appengine.subscriptionUniqueToken
      property with a unique alphanumeric combination of your choice.

- To enable logging of HTTP requests and responses (highly recommended
  when developing), please take a look at logging.properties. See also
  [the LogManager documentation][1].

## Running and Deploying Your Application from the Command Line

### To deploy your application to appspot.com:

You can deploy the application by running:

```
$ mvn gcloud:deploy-Dgcloud.version=1 -DskipTests=true
```

If this is the first time you have run "update" on the project, a
browser window will open prompting you to log in. Log in with the same
Google account the app is registered with.

Then access the following URL:

https://{your-application-id}.appspot.com/

### To run your application locally on a development server:

- Set the following environment variable

  - GOOGLE_APPLICATION_CREDENTIALS: the file path to the downloaded JSON file.

Then start your server by running:

```
$ gcloud config set project <your-application-id>
$ mvn gcloud:run
```

## Setup Project in Eclipse

Prerequisites: install Eclipse, Google Plugin for Eclipse, and the
maven m2e plugin.

### Setup Eclipse Preferences

- Window > Preferences... (or on Mac, Eclipse > Preferences...)
- Select Maven
    - check on "Download Artifact Sources"
    - check on "Download Artifact JavaDoc"
- Import pubsub-appengine-sample project
- File > Import...
- Select "Maven > Existing Maven Projects" and click "Next"
- Click "Browse" next to "Select root directory", find
  [someDirectory]/pubsub-appengine-sample and click "Next"
- Click "Finish"

### Run
- Right-click on project cloud-pubsub-appengine-sample
- Run As > Maven build -> gcloud:run

[1]: http://docs.oracle.com/javase/6/docs/api/java/util/logging/LogManager.html
