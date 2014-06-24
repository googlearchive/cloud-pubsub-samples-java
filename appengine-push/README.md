Instructions for the Cloud Pub/Sub App Engine Sample

Note: The push endpoints don't work with the App Engine's local
devserver. The push notifications will go to an HTTP URL on the App
Engine server even when you run this sample locally. So we recommend
you deploy and run the app on App Engine.
TODO(tmatsuo): Better implementation for devserver.

= Register Your Application

* Go to https://cloud.google.com/console/project and create a new
  project. This will automatically enable an App Engine application
  with the same ID as the project. We will use this App Engine
  application ID later, so make note of the application ID.
* Enable the "Google Cloud Pub/Sub" API under "APIs & auth > APIs."
* For local development, also create a new client ID of type Service
  account. Save the private key in a secure location and make note of
  the service account email address.

= Prerequisites: install Java 7, and Maven 3.1.0 or higher. You may
need to set your JAVA_HOME

> cd [someDirectory]
> unzip [theCodeZipFile]
> mvn clean package

* Edit src/main/webapp/WEB-INF/appengine-web.xml, and:
    * Enter the unique application ID (you chose it in the prior step)
      between the <application> tags.
    * Set the
      com.google.cloud.pubsub.client.demos.appengine.subscriptionUniqueToken
      property with a unique alphanumeric combination of your choice.

* To enable logging of HTTP requests and responses (highly recommended
  when developing), please take a look at logging.properties. See also
  [the LogManager documentation][1].

= Running and Deploying Your Application from the Command Line

== To deploy your application to appspot.com:

You can deploy the application by running:

    $ mvn appengine:update

If this is the first time you have run "update" on the project, a
browser window will open prompting you to log in. Log in with the same
Google account the app is registered with.

Then access the following URL:

https://{your-application-id}.appspot.com/

== To run your application locally on a development server:

Edit the appengine-web.xml file with the following 2 changes:

* Fill your service account e-mail address in
  com.google.cloud.pubsub.client.demos.appengine.serviceAccountEmail
* Provide the path of your p12 private key file in
  com.google.cloud.pubsub.client.demos.appengine.p12certificatePath
  Your web application working directory is based on the
  'src/main/webapp/WEB-INF' folder, so you can just put your p12 file
  in it and only reference the file in the form of
  'WEB-INF/{FILENAME}' in the appengine-web.xml.

Then start your server by running:

    $ mvn appengine:devserver

= Setup Project in Eclipse

Prerequisites: install Eclipse, Google Plugin for Eclipse, and the
maven m2e plugin.

= Setup Eclipse Preferences

* Window > Preferences... (or on Mac, Eclipse > Preferences...)
* Select Maven
    * check on "Download Artifact Sources"
    * check on "Download Artifact JavaDoc"
* Import pubsub-appengine-sample project
* File > Import...
* Select "Maven > Existing Maven Projects" and click "Next"
* Click "Browse" next to "Select root directory", find
  [someDirectory]/pubsub-appengine-sample and click "Next"
* Click "Finish"

== Run
* Right-click on project cloud-pubsub-appengine-sample
* Run As > Maven build -> appengine:devserver

[1]: http://docs.oracle.com/javase/6/docs/api/java/util/logging/LogManager.html