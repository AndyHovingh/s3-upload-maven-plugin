package com.bazaarvoice.maven.plugins.s3.upload;

import cloud.localstack.DockerTestUtils;
import cloud.localstack.TestUtils;
import cloud.localstack.docker.LocalstackDockerExtension;
import cloud.localstack.docker.annotation.IEnvironmentVariableProvider;
import cloud.localstack.docker.annotation.LocalstackDockerProperties;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Sets;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugin.logging.SystemStreamLog;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(LocalstackDockerExtension.class)
@LocalstackDockerProperties(
        services = { "s3:4572" },
        environmentVariableProvider = S3UploadMojoIT.LocalstackEnvironment.class
)
class S3UploadMojoIT
{
    public static class LocalstackEnvironment implements IEnvironmentVariableProvider
    {
        @Override
        public Map<String, String> getEnvironmentVariables()
        {
            return Collections.singletonMap("DEFAULT_REGION", S3UploadMojoIT.REGION);
        }
    }

    private static final String ACCESS_KEY_ID = TestUtils.TEST_ACCESS_KEY;
    private static final String SECRET_ACCESS_KEY = TestUtils.TEST_SECRET_KEY;
    private static final String REGION = TestUtils.DEFAULT_REGION;
    private static final String ENDPOINT = "http://localhost:4572";
    private static final String BUCKET = "test-bucket";
    private static final File FILE = new File("src/test/resources/file.txt");
    private static final File FOLDER = new File("src/test/resources/folder");
    private static final File TOP_LEVEL_FILE_1 =
            new File(FOLDER.getAbsolutePath() + "/top-level-file-1.txt");
    private static final File TOP_LEVEL_FILE_2 =
            new File(FOLDER.getAbsolutePath() + "/top-level-file-2.txt");
    private static final File LOW_LEVEL_FILE_1 =
            new File(FOLDER.getAbsolutePath() + "/subfolder/low-level-file-1.txt");
    private static final File LOW_LEVEL_FILE_2 =
            new File(FOLDER.getAbsolutePath() + "/subfolder/low-level-file-2.txt");

    private static final Log log = new SystemStreamLog();

    private static File tempFolder;

    private static void setCredentialsViaJavaSystemProperties()
    {
        System.setProperty(SDKGlobalConfiguration.ACCESS_KEY_SYSTEM_PROPERTY, ACCESS_KEY_ID);
        System.setProperty(SDKGlobalConfiguration.SECRET_KEY_SYSTEM_PROPERTY, SECRET_ACCESS_KEY);
    }

    private static void setRegionViaJavaSystemProperties()
    {
        System.setProperty(SDKGlobalConfiguration.AWS_REGION_SYSTEM_PROPERTY, REGION);
    }

    private static S3UploadMojo.Builder mojoBuilder()
    {
        return new S3UploadMojo.Builder()
                .withEndpoint(ENDPOINT)
                .withLog(log) // would be injected by Maven
                .withS3Provider(new S3UploadMojo.S3Provider(
                        AmazonS3Client.builder()
                                      // virtual-hosted-style is the default.
                                      //
                                      // path-style is required for localstack
                                      // https://github.com/localstack/localstack/issues/1512
                                      // https://github.com/localstack/localstack/issues/43
                                      //
                                      // path-style is being deprecated
                                      // https://forums.aws.amazon.com/ann.jspa?annID=6776
                                      .enablePathStyleAccess()
                ));
    }

    private static String checksum(File file) throws IOException, NoSuchAlgorithmException
    {
        final MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(Files.readAllBytes(file.toPath()));
        final byte[] digest = md.digest();
        return DatatypeConverter.printHexBinary(digest).toUpperCase();
    }

    private static void assertFileContentIsTheSame(
            final AmazonS3 s3,
            final String key,
            final File originalFile
    ) throws IOException, NoSuchAlgorithmException
    {
        final File downloadedFile = new File(
                tempFolder.getAbsolutePath() + "/" + originalFile.getName()
        );
        downloadedFile.deleteOnExit();
        s3.getObject(
                new GetObjectRequest(BUCKET, key),
                downloadedFile
        );
        assertEquals(
                checksum(originalFile),
                checksum(downloadedFile),
                "The S3 downloaded (from localstack s3://" +
                BUCKET + "/" + key +
                ") file's MD5 checksum " +
                "does not match the original file " +
                originalFile.getAbsolutePath()
        );
    }

    private static boolean keyExists(
            final AmazonS3 s3,
            final String key
    )
    {
        try {
            s3.getObjectMetadata(
                    BUCKET,
                    key
            );
            return true;
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                return true;
            }
            throw e;
        }
    }

    private static Set<String> listKeys(
            final AmazonS3 s3,
            final String prefix
    )
    {
        final Set<String> keys = new HashSet<>();
        for (S3ObjectSummary summary : s3.listObjects(BUCKET, prefix).getObjectSummaries()) {
            keys.add(summary.getKey());
        }
        return keys;
    }

    private static <T> void assertSetsAreTheSame(
            final String setName,
            final Set<T> expected,
            final Set<T> actual
    )
    {
        String message = "";
        final Sets.SetView<T> missing = Sets.difference(expected, actual);
        if (!missing.isEmpty()) {
            message += setName + " was missing " + missing.size() + " elements: " +
                       missing.toString() + ". ";
        }
        final Sets.SetView<T> unexpected = Sets.difference(actual, expected);
        if (!unexpected.isEmpty()) {
            message += setName + " had " + unexpected.size() + " unexpected elements: " +
                       unexpected.toString() + ". ";
        }
        if (!message.isEmpty()) {
            fail(message);
        }
    }

    @BeforeAll
    static void beforeAll() throws IOException
    {
        tempFolder = Files.createTempDirectory("").toFile();
        tempFolder.deleteOnExit();
        log.info("using tempFolder '" + tempFolder.getAbsolutePath() + "'");

        final AmazonS3 s3 = DockerTestUtils.getClientS3();
        s3.createBucket(BUCKET);
        log.info("BUCKET '" + BUCKET + "' created");
    }

    @BeforeEach
    void beforeEach()
    {
        setCredentialsViaJavaSystemProperties();
        setRegionViaJavaSystemProperties();
    }

    @Test
    void itUploadsAFileWithDefaultConfig() throws MojoExecutionException, IOException, NoSuchAlgorithmException
    {
        final String destinationKey = "uploads-a-file-with-default-config/file.txt";

        final S3UploadMojo mojo = mojoBuilder()
                .withSource(FILE)
                .withBucketName(BUCKET)
                .withDestination(destinationKey)
                .build();

        mojo.execute();

        assertFileContentIsTheSame(
                DockerTestUtils.getClientS3(),
                destinationKey,
                FILE
        );
    }

    @Test
    void itDoesNotUploadAFileInDryRun() throws MojoExecutionException
    {
        final String destinationKey = "does-not-upload-in-dry-run/file.txt";

        final S3UploadMojo mojo = mojoBuilder()
                .withSource(FILE)
                .withBucketName(BUCKET)
                .withDestination(destinationKey)
                .withDoNotUpload(true)
                .build();

        mojo.execute();

        if (!keyExists(DockerTestUtils.getClientS3(), destinationKey)) {
            fail("Expected s3://" + BUCKET + "/" + destinationKey + " not to exist");
        }
    }

    @Test
    void itUploadsADirectoryWithDefaultConfig() throws MojoExecutionException, IOException, NoSuchAlgorithmException
    {
        final String destinationKey = "uploads-a-folder-with-default-config";

        final S3UploadMojo mojo = mojoBuilder()
                .withSource(FOLDER)
                .withBucketName(BUCKET)
                .withDestination(destinationKey)
                .build();

        mojo.execute();

        final AmazonS3 s3 = DockerTestUtils.getClientS3();

        assertSetsAreTheSame(
                "Uploaded set of keys from folder",
                Sets.newHashSet(
                        destinationKey + "/" + TOP_LEVEL_FILE_1.getName(),
                        destinationKey + "/" + TOP_LEVEL_FILE_2.getName()
                ),
                listKeys(s3, destinationKey + "/")
        );
        assertFileContentIsTheSame(
                s3,
                destinationKey + "/" + TOP_LEVEL_FILE_1.getName(),
                TOP_LEVEL_FILE_1
        );
        assertFileContentIsTheSame(
                s3,
                destinationKey + "/" + TOP_LEVEL_FILE_2.getName(),
                TOP_LEVEL_FILE_2
        );
    }

    @Test
    void itUploadsADirectoryRecursively() throws MojoExecutionException, IOException, NoSuchAlgorithmException
    {
        final String destinationKey = "uploads-a-folder-recursively";

        final S3UploadMojo mojo = mojoBuilder()
                .withSource(FOLDER)
                .withBucketName(BUCKET)
                .withDestination(destinationKey)
                .withRecursive(true)
                .build();

        mojo.execute();

        final AmazonS3 s3 = DockerTestUtils.getClientS3();

        assertSetsAreTheSame(
                "Uploaded set of keys from folder",
                Sets.newHashSet(
                        destinationKey + "/" + TOP_LEVEL_FILE_1.getName(),
                        destinationKey + "/" + TOP_LEVEL_FILE_2.getName(),
                        destinationKey + "/subfolder/" + LOW_LEVEL_FILE_1.getName(),
                        destinationKey + "/subfolder/" + LOW_LEVEL_FILE_2.getName()
                ),
                listKeys(s3, destinationKey + "/")
        );
        assertFileContentIsTheSame(
                s3,
                destinationKey + "/" + TOP_LEVEL_FILE_1.getName(),
                TOP_LEVEL_FILE_1
        );
        assertFileContentIsTheSame(
                s3,
                destinationKey + "/" + TOP_LEVEL_FILE_2.getName(),
                TOP_LEVEL_FILE_2
        );
        assertFileContentIsTheSame(
                s3,
                destinationKey + "/subfolder/" + LOW_LEVEL_FILE_1.getName(),
                LOW_LEVEL_FILE_1
        );
        assertFileContentIsTheSame(
                s3,
                destinationKey + "/subfolder/" + LOW_LEVEL_FILE_2.getName(),
                LOW_LEVEL_FILE_2
        );
    }
}
