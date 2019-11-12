package com.bazaarvoice.maven.plugins.s3.upload;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.*;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;

@Mojo(name = "s3-upload")
public class S3UploadMojo extends AbstractMojo
{
  private static final Long DELAY_BETWEEN_SHOW_UPDATES_MS = 250L;

  public static class S3Provider
  {
    private final AmazonS3ClientBuilder builder;

    S3Provider()
    {
      this(AmazonS3Client.builder());
    }

    S3Provider(final AmazonS3ClientBuilder builder)
    {
      this.builder = builder;
    }

    public AmazonS3 getS3(
            final AWSCredentials credentialsMaybe,
            final String endpointMaybe,
            final String regionMaybe,
            final String profileMaybe
    )
    {
      final AWSCredentialsProvider credentials;
      if (credentialsMaybe != null) {
        credentials = new AWSStaticCredentialsProvider(credentialsMaybe);
      } else if (profileMaybe != null) {
        credentials = new ProfileCredentialsProvider(profileMaybe);
      } else {
        credentials = new DefaultAWSCredentialsProviderChain();
      }
      builder.setCredentials(credentials);

      if (regionMaybe != null) {
        builder.setRegion(regionMaybe);
      }

      if (endpointMaybe != null) {
        builder.setEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(
                        endpointMaybe,
                        regionMaybe == null
                          ? new DefaultAwsRegionProviderChain().getRegion()
                          : regionMaybe
                )
        );
      }
      return builder.build();
    }
  }

  public static class Builder
  {
    private String accessKey;
    private String secretKey;
    private boolean doNotUpload;
    private File source;
    private String bucketName;
    private String destination;
    private String endpoint;
    private String region;
    private String profile;
    private boolean recursive;
    private S3Provider s3Provider;
    private boolean showProgress = true;
    private Log log;

    public Builder withAccessKey(final String accessKey)
    {
      this.accessKey = accessKey;
      return this;
    }

    public Builder withSecretKey(final String secretKey)
    {
      this.secretKey = secretKey;
      return this;
    }

    public Builder withDoNotUpload(final Boolean doNotUpload)
    {
      this.doNotUpload = doNotUpload;
      return this;
    }

    public Builder withSource(final File source)
    {
      this.source = source;
      return this;
    }

    public Builder withBucketName(final String bucketName)
    {
      this.bucketName = bucketName;
      return this;
    }

    public Builder withDestination(final String destination)
    {
      this.destination = destination;
      return this;
    }

    public Builder withEndpoint(final String endpoint)
    {
      this.endpoint = endpoint;
      return this;
    }

    public Builder withRegion(final String region)
    {
      this.region = region;
      return this;
    }

    public Builder withProfile(final String profile)
    {
      this.profile = profile;
      return this;
    }

    public Builder withRecursive(final Boolean recursive)
    {
      this.recursive = recursive;
      return this;
    }

    public Builder withShowProgress(final Boolean showProgress)
    {
      this.showProgress = showProgress;
      return this;
    }

    public Builder withLog(final Log log)
    {
      this.log = log;
      return this;
    }

    public Builder withS3Provider(final S3Provider s3Provider)
    {
      this.s3Provider = s3Provider;
      return this;
    }

    public S3UploadMojo build()
    {
      return new S3UploadMojo(this);
    }
  }

  /** no-arg constructor for Maven */
  @SuppressWarnings("unused")
  public S3UploadMojo() {}

  private S3UploadMojo(final Builder builder)
  {
    accessKey = builder.accessKey;
    secretKey = builder.secretKey;
    doNotUpload = builder.doNotUpload;
    source = builder.source;
    bucketName = builder.bucketName;
    destination = builder.destination;
    endpoint = builder.endpoint;
    region = builder.region;
    profile = builder.profile;
    recursive = builder.recursive;
    showProgress = builder.showProgress;
    setLog(builder.log);
    s3Provider = builder.s3Provider;
  }

  /** Access key for S3. */
  @Parameter(property = "s3-upload.accessKey")
  private String accessKey;

  /** Secret key for S3. */
  @Parameter(property = "s3-upload.secretKey")
  private String secretKey;

  /**
   *  Execute all steps up except the upload to the S3.
   *  This can be set to true to perform a "dryRun" execution.
   */
  @Parameter(property = "s3-upload.doNotUpload", defaultValue = "false")
  private boolean doNotUpload;

  /** The file/folder to upload. */
  @Parameter(property = "s3-upload.source", required = true)
  private File source;

  /** The bucket to upload into. */
  @Parameter(property = "s3-upload.bucketName", required = true)
  private String bucketName;

  /** The file/folder (in the bucket) to create. */
  @Parameter(property = "s3-upload.destination", required = true)
  private String destination;

  /** Force override of endpoint for S3 regions such as EU. */
  @Parameter(property = "s3-upload.endpoint")
  private String endpoint;

  /** Region of the destination bucket */
  @Parameter(property = "s3-upload.region")
  private String region;

  /** AWS Profile to get credentials */
  @Parameter(property = "s3-upload.profile")
  private String profile;

  /** In the case of a directory upload, recursively upload the contents. */
  @Parameter(property = "s3-upload.recursive", defaultValue = "false")
  private boolean recursive;

  /** To show upload detail/progress while upload is in progress */
  @Parameter(property = "s3-upload.showProgress", defaultValue = "true")
  private boolean showProgress;

  private S3Provider s3Provider = new S3Provider();

  @Override
  public void execute() throws MojoExecutionException
  {
    if (!source.exists()) {
      throw new MojoExecutionException("File/folder doesn't exist: " + source);
    }

    final AmazonS3 s3 = s3Provider.getS3(
            accessKey != null && secretKey != null
              ? new BasicAWSCredentials(accessKey, secretKey)
              : null,
            endpoint,
            region,
            profile
    );

    if (!s3.doesBucketExistV2(bucketName)) {
      throw new MojoExecutionException("Bucket doesn't exist: " + bucketName);
    }

    if (doNotUpload) {
      getLog().info(String.format("File %s would have be uploaded to s3://%s/%s (dry run)",
              source, bucketName, destination));

      return;
    }

    final boolean success = upload(s3, source);
    if (!success) {
      throw new MojoExecutionException("Unable to upload file to S3.");
    }

    getLog().info(String.format("File %s uploaded to s3://%s/%s",
            source, bucketName, destination));
  }

  private boolean upload(
          final AmazonS3 s3,
          final File sourceFile
  ) throws MojoExecutionException
  {
    final TransferManager mgr =
            TransferManagerBuilder.standard().withS3Client(s3).build();

    final Transfer transfer;
    if (sourceFile.isFile()) {
      transfer = mgr.upload(new PutObjectRequest(bucketName, destination, sourceFile)
              .withCannedAcl(CannedAccessControlList.BucketOwnerFullControl));
    } else if (sourceFile.isDirectory()) {
      transfer = mgr.uploadDirectory(bucketName, destination, sourceFile, recursive,
              new ObjectMetadataProvider() {
                @Override
                public void provideObjectMetadata(final File file, final ObjectMetadata objectMetadata) {
                  /**
                   * This is a terrible hack, but the SDK as of 1.10.69 does not allow setting ACLs
                   * for directory uploads otherwise.
                   */
                  objectMetadata.setHeader(Headers.S3_CANNED_ACL, CannedAccessControlList.BucketOwnerFullControl);
                }
              });
    } else {
      throw new MojoExecutionException("File is neither a regular file nor a directory " + sourceFile);
    }
    try {
      getLog().debug("Transferring " + transfer.getProgress().getTotalBytesToTransfer() + " bytes...");
      if (showProgress) {
        while (!transfer.isDone()) {
          Thread.sleep(DELAY_BETWEEN_SHOW_UPDATES_MS);
          TransferProgress progress = transfer.getProgress();
        }
      }
      transfer.waitForCompletion();
      getLog().info("Transferred " + transfer.getProgress().getBytesTransferred() + " bytes.");
    } catch (InterruptedException e) {
      return false;
    }

    return transfer.getState() == Transfer.TransferState.Completed;
  }
}
