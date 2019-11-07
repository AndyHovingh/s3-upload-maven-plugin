s3-upload-maven-plugin
======================
Uploads a file or (recursively) the contents of a directory to S3.

Minimum Requirements
------------------------
Java Version: 7+
Maven Version: 3.3.9+

Configuration parameters
------------------------

| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
|bucketName|The name of the bucket|*yes*| |
|source|The source file or folder (was sourceFile before 1.2)|*yes*| |
|destination|The destination file or destination folder (was destinationFile before 1.2)| *yes*| |
|recursive|If this is a directory copy, recursively copy all contents (since 1.2)| *no* | false |
|accessKey|S3 access key | *no* | if unspecified, uses the Default Provider, falling back to env variables |
|secretKey|S3 secret key | *no* | if unspecified, uses the Default Provider, falling back to env variables |
|doNotUpload|Dry run| *no* | false |
|endpoint|Use a different s3 endpoint| *no* | s3.amazonaws.com |

Example: Upload a file
----------------------
```xml
<build>
  ...

  <plugins>
    ...

    <plugin>
      <groupId>com.bazaarvoice.maven.plugins</groupId>
      <artifactId>s3-upload-maven-plugin</artifactId>
      <version>1.5.0</version>
      <configuration>
        <bucketName>my-s3-bucket</bucketName>
        <source>dir/filename.txt</source>
        <destination>remote-dir/remote-filename.txt</destination>
      </configuration>
    </plugin>
  </plugins>
</build>
```

Example: Recursively upload a folder
------------------------------------
```xml
<build>
  ...

  <plugins>
    ...

    <plugin>
      <groupId>com.bazaarvoice.maven.plugins</groupId>
      <artifactId>s3-upload-maven-plugin</artifactId>
      <version>1.5.0</version>
      <configuration>
        <bucketName>my-s3-bucket</bucketName>
        <source>dir</source>
        <destination>remote-dir</destination>
        <recursive>true</recursive>
      </configuration>
    </plugin>
  </plugins>
</build>
```

Plugin Development
------------------------

#### check dependencies are up-to-date

```bash
$ mvn versions:display-dependency-updates versions:display-plugin-updates
```

## Changelog

Please see [Release Notes](https://github.com/bazaarvoice/s3-upload-maven-plugin/releases) for a list of changes.

## Releases

We follow semantic versioning policy as of version 1.5.0 ([semver.org](http://semver.org/)):

> Given a version number MAJOR.MINOR.PATCH, increment the:
>
>MAJOR version when you make incompatible API changes,<br/>
>MINOR version when you add functionality in a backwards-compatible manner, and<br/>
>PATCH version when you make backwards-compatible bug fixes.
