@ECHO OFF
%JAVA_HOME%\bin\java -cp ^
%~dp0\..\target\pubsub-pull-sample-1.0-jar-with-dependencies.jar ^
com.google.cloud.pubsub.client.demos.cli.Main ^
%*