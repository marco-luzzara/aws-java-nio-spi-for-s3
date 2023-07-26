package software.amazon.nio.spi.s3;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.nio.spi.s3.config.S3NioSpiConfiguration;

import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class S3FileSystemIT {
    private static final String BUCKET = "test";
    private static final String BASE_URI = "%s://%s".formatted(S3FileSystemProvider.SCHEME, BUCKET);
//    private static final S3FileSystem fs;

    private static S3Client s3;

    @Container
    private static final LocalStackContainer localstack = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:2.1.0")).withServices(LocalStackContainer.Service.S3);

    private static String buildS3Uri(String keyPath) {
        return "%s/%s".formatted(BASE_URI, keyPath);
    }

    @BeforeAll
    static void initializeAll() {
        s3 = S3Client
                .builder()
                .endpointOverride(localstack.getEndpoint())
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())
                        )
                )
                .region(Region.of(localstack.getRegion()))
                .build();
        s3.createBucket(b -> b.bucket(BUCKET));
    }

    @Test
    void givenBytesToWriteOnFileInRoot_whenFileIsCreated_thenObjectIsAddedToBucket() throws IOException {
        try (var fs = FileSystems.newFileSystem(URI.create(BASE_URI), getConfigsWithLocalstackParams())) {
            var root = getRoot(fs);
            var filePath = root.resolve("test.txt");
            final var fileContent = new byte[] { 1, 2 };

            Files.createFile(filePath);
            Files.write(filePath, fileContent);

            assertThat(getObjectContent(BUCKET, "test.txt")).isEqualTo(fileContent);
            assertThat(getObjectCount(BUCKET)).isEqualTo(1);
        }
    }

    @Test
    void givenBytesToWriteOnFileInNestedFolder_whenFileIsCreated_thenObjectIsAddedToBucket() throws IOException {
        try (var fs = FileSystems.newFileSystem(URI.create(BASE_URI), getConfigsWithLocalstackParams())) {
            var root = getRoot(fs);
            final var dirPath = root.resolve("folder1").resolve("folder2");
            final var filePath = dirPath.resolve("test.txt");
            final var fileContent = new byte[] { 1, 2 };

            Files.createDirectories(dirPath);
            Files.createFile(filePath);
            Files.write(filePath, fileContent);

            assertThat(getObjectContent(BUCKET, "folder1/folder2/test.txt")).isEqualTo(fileContent);
            // there is a key for the folder + the file key
            assertThat(getObjectCount(BUCKET)).isEqualTo(2);
        }
    }

    @Test
    void givenObjectUploadedInS3_whenItIsAccessedAsFile_thenReturnItsContent() throws IOException {
        try (var fs = FileSystems.newFileSystem(URI.create(BASE_URI), getConfigsWithLocalstackParams())) {
            final var fileContent = new byte[] { 1, 2 };
            uploadObject(BUCKET, "test.txt", fileContent);
            var root = getRoot(fs);
            final var filePath = root.resolve("test.txt");

            var readContent = Files.readAllBytes(filePath);

            assertThat(readContent).isEqualTo(fileContent);
        }
    }

    @Test
    void givenManyUploadedInSameDir_whenListingTheDir_thenReturnThem() throws IOException {
        try (var fs = FileSystems.newFileSystem(URI.create(BASE_URI), getConfigsWithLocalstackParams())) {
            var root = getRoot(fs);
            final var dirPath = root.resolve("mydir");
            uploadObject(BUCKET, "mydir/test1.txt", new byte[] {});
            uploadObject(BUCKET, "mydir/test2.txt", new byte[] {});
            uploadObject(BUCKET, "mydir/nested/test3.txt", new byte[] {});
            uploadObject(BUCKET, "excluded/not.txt", new byte[] {});

            var files = Files.walk(dirPath.resolve(".")).toList();

            assertThat(files).extracting(Path::toString).containsOnly("/mydir/.", "mydir/nested/",
                    "mydir/test1.txt", "mydir/test2.txt", "mydir/nested/test3.txt");
        }
    }

    private Path getRoot(FileSystem fs) {
        return fs.getRootDirectories().iterator().next();
    }

    private void uploadObject(String bucket, String key, byte[] fileContent) {
        s3.putObject(b -> b.bucket(bucket).key(key), RequestBody.fromBytes(fileContent));
    }

    private byte[] getObjectContent(String bucket, String path) {
        return s3.getObjectAsBytes(b -> b.bucket(bucket).key(path)).asByteArray();
    }

    private int getObjectCount(String bucket) {
        return s3.listObjectsV2(b -> b.bucket(bucket)).keyCount();
    }

    private S3NioSpiConfiguration getConfigsWithLocalstackParams() {
        return new S3NioSpiConfiguration().withEndpoint(localstack.getEndpoint().toString())
                .withRegion(Region.of(localstack.getRegion()))
                .withAccessKey(localstack.getAccessKey())
                .withSecretKey(localstack.getSecretKey());
    }

    @AfterEach
    void cleanupEach() {
        var objects = s3.listObjectsV2(b -> b.bucket(BUCKET)).contents();
        s3.deleteObjects(b -> b.bucket(BUCKET).delete(delBuilder -> delBuilder.objects(
                objects.stream()
                        .map(o -> ObjectIdentifier.builder().key(o.key()).build())
                        .collect(Collectors.toSet()))));
    }
}
