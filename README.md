[![Java Version](http://img.shields.io/badge/Java-1.8-blue.svg)](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
[![Maven Version](http://img.shields.io/maven-central/v/tdl/s3-sync-stream.svg)](http://search.maven.org/#search%7Cgav%7C1%7Cg%3A%22ro.ghionoiu%22%20AND%20a%3A%22s3-sync-stream%22)
[![Codeship Status for julianghionoiu/s3-sync-stream](https://img.shields.io/codeship/b617e390-006f-0135-fe1b-4ee982914aba/master.svg)](https://codeship.com/projects/212588)
[![Coverage Status](https://coveralls.io/repos/github/julianghionoiu/s3-sync-stream/badge.svg?branch=master)](https://coveralls.io/github/julianghionoiu/s3-sync?branch=master)

Library that continuously syncs the contents of a folder to an S3 bucket. Optimised for streaming file formats (video, logs).

The library will aggresively try to upload content as it is being generated:
* Each chunk will be uploaded as a part in a multipart upload.
* The multipart upload in kept open while the file is being generated. 
* To signal the fact that file is being generated, the generator has to create a `.lock` with the same name.
* Once the file generation is completed and the `.lock` file removed, the multipart upload will be finalised.


## To use as a library

### Add as Maven dependency

Add a dependency to `tdl:s3-sync-stream` in test scope. (Note: 0.0.3 is the latest stable version as of the latest edit on this page.)
```xml
<dependency>
  <groupId>tdl</groupId>
  <artifactId>s3-sync-stream</artifactId>
  <version>0.0.3</version>
</dependency>
```

### Define sync source and destination

Configure the local folder as a `source` and define AWS S3 as the `destination`
```java
Source source = Source.getBuilder(/* Path */ pathToFolder)
  .traverseDirectories(true)
  .include(endsWith(".mp4")
  .exclude(startsWith(".")
  .exclude(matches("tmp.log"))
  .create();

Destination destination = Destination.getBuilder()
  .loadFromPath(/* Path */ pathToFile)
  .create();
```

Construct the `RemoteSync` and run. The `run` method can be invoked multiple times.
```java
remoteSync = new RemoteSync(source, destination);
remoteSync.run();
```

### Example source definitions

The source will be a set of filters that can be applied to a folder to obtain a list of files to be synced

**Default values** will not include .lock files and hidden files (. files)
```java
Source source = Source.getBuilder(/* Path */ pathToFolder)
  .includeAll()
  .create();
```

**Single file** can be selected using a matcher
```java
Filter filter = Filter.getBuilder().matches("file.txt");

Source source = Source.getBuilder(/* Path */ pathToFolder)
  .include(filter)
  .create();
```

**Multiple files** can be included if they match one of the matchers.
The list of included files can be further filtered via exclude matchers
```java
Filter includeFilter = Filter.getBuilder()
                        .endsWith(".mp4")
                        .endsWith(".log")
                        .create();

Filter excludeFilter = Filter.getBuilder()
                        .matches("tmp.log")
                        .create();

Source source = Source.getBuilder(/* Path */ pathToFolder)
  .include(includeFilter)
  .exclude(excludeFilter)
  .create();
```

By default the library will not **traverse directories**, if you need this behaviour than you can set the `traverseDirectories` flag to true
```java
Source source = Source.getBuilder(/* Path */ pathToFolder)
  .traverseDirectories(true)
  .includeAll()
  .create();
```

If no include matcher is specified then an **IllegalArgumentException** will be raised upon creation:
```java
Source source = Source.getBuilder(/* Path */ pathToFolder)
  .create();
```

## Development

### Prepare environment

Configuration for running this service should be placed in file `.private/aws-test-secrets` in Java Properties file format. For examples.

```properties
aws_access_key_id=ABCDEFGHIJKLM
aws_secret_access_key=ABCDEFGHIJKLM
s3_region=ap-southeast-1
s3_bucket=bucketname
s3_prefix=prefix
```

The available values are
1. `aws_access_key_id`
    This contains the access key to the AWS account.
2. `aws_secret_access_key`
    This contains the secret key to the AWS account.
3. `s3_region`
    This contains the region that holds the S3 bucket.
4. `s3_bucket`
    This contains the bucket that will store the uploaded files.
5. `s3_prefix`
    This contains optional prefix for the base storage.

### Build and run as command-line app
```bash
./gradlew shadowJar
java -jar ./build/libs/s3-sync-1.0-SNAPSHOT-all.jar \
    -c config.properties \
    -d $PATH_TO_REC/ \
    -R \
    --filter "^[0-9a-zA-Z\\_]+\\.txt$"
```