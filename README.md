# Prototype of async, non-blocking aws-java-sdk client

Repository contains my results of tinkering with aws-java-sdk library
and async-http-client to build thin layer on top
of amazon sdk to get real non-blocking client

Main goals

1. Investigate how hard is to creat non-blocking client for aws sdk (not hard, but requires some copypaste now due to private methods)
2. Create working client for one of aws services - dynamodb

**Right now this project does not have plans to cover other aws modules as
according to [this comment](https://github.com/aws/aws-sdk-java/issues/725#issuecomment-308870416) 
 AWS team has plans to publish official non-blocking support**. But as I started before,
 this announcement and they have not published anything yet, I don't want to throw
 away my progress in trash bin now :).
 
## Modules

1. aws-async-core - non-blocking analog of AmazonWebServiceClient, that is used internally by all aws services
2. aws-async-dynamodb - non-blocking client for Amazon DynamoDB
3. example - simple example app showing that non-blocking client works
 
## Further development
I'm really hoping that it will not be required as AWS sdk team will release their own
non-blocking client that will be feature rich and maintained.

In case it will not happen, roadmap could be:
1. Support other aws-sdk modules by utilizing sdk client generator (yes, all clients are generated code, mine now is handwritten)
2. Fix shortcuts in WebServiceClients - metrics, retries
3. Non-blocking versions for high level parts of sdk, e.g. dynamodb mapper

## Licence
Apache 2.0