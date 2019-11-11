package com.bazaarvoice.maven.plugins.s3.upload;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugin.logging.SystemStreamLog;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class S3UploadMojoTest
{
    private static final AWSCredentials CREDENTIALS = new BasicAWSCredentials(
            "TEST_ACCESS_KEY_ID",
            "TEST_SECRET_KEY"
    );
    private static final AWSCredentials DIFFERENT_CREDENTIALS = new BasicAWSCredentials(
            "DIFFERENT_ACCESS_KEY_ID",
            "DIFFERENT_SECRET_KEY"
    );
    private static final File FILE = new File("src/test/resources/file.txt");
    private static final String BUCKET = "test-bucket";
    private static final String DESTINATION_KEY = "unit-test-destination/key";

    private static final Log log = new SystemStreamLog();

    private static AmazonS3 mockS3()
    {
        final AmazonS3 mockS3 = mock(AmazonS3.class);

        when(mockS3.doesBucketExistV2(BUCKET)).thenReturn(true);

        final PutObjectResult putObjectResult = new PutObjectResult();
        when(mockS3.putObject(any(PutObjectRequest.class))).thenReturn(putObjectResult);

        return mockS3;
    }

    private static S3UploadMojo.Builder mojoBuilder(
            final AmazonS3ClientBuilder spyS3Builder
    )
    {
        Mockito.doReturn(mockS3()).when(spyS3Builder).build();
        return new S3UploadMojo.Builder()
                .withLog(log) // would be injected by Maven
                .withS3Provider(new S3UploadMojo.S3Provider(spyS3Builder));
    }

    @Test
    void itAppliesExplicitlyProvidedCredentials() throws MojoExecutionException
    {
        System.setProperty(
                SDKGlobalConfiguration.ACCESS_KEY_SYSTEM_PROPERTY,
                DIFFERENT_CREDENTIALS.getAWSAccessKeyId()
        );
        System.setProperty(
                SDKGlobalConfiguration.SECRET_KEY_SYSTEM_PROPERTY,
                DIFFERENT_CREDENTIALS.getAWSSecretKey()
        );

        final AmazonS3ClientBuilder spyS3Builder = spy(AmazonS3ClientBuilder.class);

        final S3UploadMojo mojo = mojoBuilder(spyS3Builder)
                .withAccessKey(CREDENTIALS.getAWSAccessKeyId())
                .withSecretKey(CREDENTIALS.getAWSSecretKey())
                .withSource(FILE)
                .withBucketName(BUCKET)
                .withDestination(DESTINATION_KEY)
                .build();

        mojo.execute();

        final AWSCredentials appliedCredentials = spyS3Builder.getCredentials().getCredentials();
        assertEquals(
                CREDENTIALS.getAWSAccessKeyId(),
                appliedCredentials.getAWSAccessKeyId(),
                "The access key id was not applied."
        );
        assertEquals(
                CREDENTIALS.getAWSSecretKey(),
                appliedCredentials.getAWSSecretKey(),
                "The secret key was not applied."
        );
    }
}
