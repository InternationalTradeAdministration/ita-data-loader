package gov.ita.dataloader.storage;

import com.microsoft.azure.storage.blob.*;
import com.microsoft.azure.storage.blob.models.BlobFlatListSegment;
import com.microsoft.azure.storage.blob.models.BlobHTTPHeaders;
import com.microsoft.azure.storage.blob.models.ContainerItem;
import com.microsoft.rest.v2.http.HttpPipeline;
import gov.ita.dataloader.HttpHelper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.microsoft.azure.storage.blob.models.DeleteSnapshotsOptionType.INCLUDE;

@Slf4j
@Service
@Profile({"production", "staging"})
public class ProductionStorage implements Storage {

  @Value("${storage-params.azure-storage-account}")
  private String accountName;

  @Value("${storage-params.azure-storage-account-key}")
  private String accountKey;

  @Autowired
  private HttpHelper httpHelper;

  @Override
  public void createContainer(String containerName) {
    log.info("Initializing container: {}", containerName);
    makeContainerUrl(containerName)
      .create(null, null, null)
      .blockingGet();
  }

  @Override
  public void save(String fileName, byte[] fileContent, String user, String containerName, Boolean userUpload, Boolean pii) {
    try {
      if (user == null) user = accountName;

      ContainerURL containerURL = makeContainerUrl(containerName);
      BlockBlobURL blobURL = containerURL.createBlockBlobURL(fileName);

      File tmpFile = File.createTempFile("tmpFile", ".tmp");
      OutputStream os = new FileOutputStream(tmpFile);
      os.write(fileContent);
      os.close();

      AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(Paths.get(tmpFile.getAbsolutePath()));
      TransferManagerUploadToBlockBlobOptions options =
        new TransferManagerUploadToBlockBlobOptions(
          null,
          makeHeader(fileName),
          makeMetaData(user, userUpload, pii),
          null,
          10);
      TransferManager.uploadFileToBlockBlob(fileChannel, blobURL, 1000000, 20000000, options).blockingGet();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public String getListBlobsUrl(String containerName) {
    return makeBaseStorageUrl(containerName) + "?restype=container&comp=list";
  }

  private String makeBaseStorageUrl(String containerName) {
    return String.format("https://%s.blob.core.windows.net/%s", accountName, containerName);
  }

  @Override
  public List<BlobMetaData> getBlobMetadata(String containerName, Boolean withSnapshots) {
    ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
    BlobListDetails details = new BlobListDetails();
    details.withMetadata(true);
    details.withSnapshots(withSnapshots);
    listBlobsOptions.withDetails(details);
    BlobFlatListSegment segment = makeContainerUrl(containerName)
      .listBlobsFlatSegment(null, listBlobsOptions, null).blockingGet().body().segment();
    if (segment != null && segment.blobItems() != null) {
      return segment.blobItems()
        .stream().map(
          x -> new BlobMetaData(
            x.name(),
            x.snapshot(),
            buildUrlForBlob(containerName, x.name(), x.snapshot()),
            x.properties().contentLength(),
            containerName,
            x.properties().lastModified(),
            x.metadata()
          )).filter(item -> !item.fileName.startsWith("adfpolybaserejectedrows"))
        .collect(Collectors.toList());
    }

    return Collections.emptyList();
  }

  @Override
  public Set<String> getContainerNames() {
    return makeServiceURL().listContainersSegment(null, null)
      .blockingGet().body()
      .containerItems().stream().map(ContainerItem::name)
      .collect(Collectors.toSet());
  }

  @Override
  public byte[] getBlob(String containerName, String blobName) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    makeContainerUrl(containerName).createBlobURL(blobName)
      .download()
      .blockingGet()
      .body(new ReliableDownloadOptions())
      .blockingForEach(b -> outputStream.write(b.array()));
    return outputStream.toByteArray();
  }

  @Override
  public byte[] getBlob(String containerName, String blobName, String snapshot) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    BlobURL blobURL = null;
    try {
      blobURL = (snapshot == null) ?
        makeContainerUrl(containerName).createBlobURL(blobName) :
        makeContainerUrl(containerName).createBlobURL(blobName).withSnapshot(snapshot);
    } catch (MalformedURLException | UnknownHostException e) {
      e.printStackTrace();
    }

    blobURL
      .download()
      .blockingGet()
      .body(new ReliableDownloadOptions())
      .blockingForEach(b -> outputStream.write(b.array()));

    return outputStream.toByteArray();
  }

  @Override
  public void makeSnapshot(String containerName, String blobName) {
    makeServiceURL().createContainerURL(containerName).createBlobURL(blobName).createSnapshot().blockingGet();
  }

  @Override
  public void delete(String containerName, String blobName, String snapshot) {
    if (snapshot == null) {
      for (BlobMetaData b : getBlobMetadata(containerName, false)) {
        if (b.getFileName().contains(blobName)) {
          log.info("Deleting blob: {}", b.getFileName());
          makeServiceURL().createContainerURL(containerName).
            createBlobURL(b.getFileName())
            .delete(INCLUDE, null, null).blockingGet();
        }
      }
    } else {
      try {
        log.info("Deleting blob snapshot: {} {}", blobName, snapshot);
        makeServiceURL().createContainerURL(containerName)
          .createBlobURL(blobName).withSnapshot(snapshot)
          .delete().blockingGet();
      } catch (MalformedURLException | UnknownHostException e) {
        e.printStackTrace();
      }
    }
  }

  private String buildUrlForBlob(String containerName, String blobName, String snapshot) {
    if (snapshot != null)
      return String.format("/api/%s/%s?snapshot=%s", containerName, blobName, snapshot);
    return String.format("/api/%s/%s", containerName, blobName);
  }

  private ContainerURL makeContainerUrl(String containerName) {
    ServiceURL serviceURL = makeServiceURL();
    assert serviceURL != null;
    return serviceURL.createContainerURL(containerName);
  }

  private ServiceURL makeServiceURL() {
    try {
      SharedKeyCredentials credential = new SharedKeyCredentials(accountName, accountKey);
      HttpPipeline pipeline = StorageURL.createPipeline(credential, new PipelineOptions());
      URL url = new URL(String.format("https://%s.blob.core.windows.net/", accountName));
      return new ServiceURL(url, pipeline);
    } catch (InvalidKeyException | MalformedURLException e) {
      e.printStackTrace();
    }
    return null;
  }

  private Metadata makeMetaData(String uploadedBy, Boolean userUpload, Boolean pii) {
    Metadata metadata = new Metadata();
    metadata.put("uploaded_by", uploadedBy);
    metadata.put("user_upload", userUpload.toString());
    metadata.put("pii", pii.toString());
    return metadata;
  }

  private BlobHTTPHeaders makeHeader(String fileName) {
    String contentType = null;

    if (fileName.endsWith(".zip"))
      contentType = "application/zip";
    if (fileName.endsWith(".json"))
      contentType = "application/json";
    if (fileName.endsWith(".csv"))
      contentType = "text/csv";
    if (fileName.endsWith(".xml"))
      contentType = "application/xml";
    if (fileName.endsWith(".txt"))
      contentType = "text/plain";

    BlobHTTPHeaders headers = new BlobHTTPHeaders();
    headers.withBlobContentType(contentType);
    return headers;
  }
}
