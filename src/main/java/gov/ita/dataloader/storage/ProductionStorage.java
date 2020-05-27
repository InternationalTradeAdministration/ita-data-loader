package gov.ita.dataloader.storage;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.*;
import gov.ita.dataloader.HttpHelper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
  public void createContainer(String containerName, Boolean isPublic) {
    log.info("Creating container: {}", containerName);
    PublicAccessType publicAccessType = (isPublic) ? PublicAccessType.BLOB : null;
    BlobContainerClient blobContainer = makeBlobServiceClient().createBlobContainer(containerName);
    blobContainer.setAccessPolicy(publicAccessType, null);
  }

  @Override
  public void save(String fileName, byte[] fileContent, String user, String containerName, Boolean userUpload, Boolean pii) {
    log.info("Saving blob {} to container {}", fileName, containerName);

    try {
      if (user == null) user = accountName;

      BlobContainerClient blobContainerClient = makeBlobServiceClient().getBlobContainerClient(containerName);
      BlobClient blobClient = blobContainerClient.getBlobClient(fileName);

      File tmpFile = File.createTempFile("tmpFile", ".tmp");
      OutputStream os = new FileOutputStream(tmpFile);
      os.write(fileContent);
      os.close();

      ParallelTransferOptions parallelTransferOptions = new ParallelTransferOptions(1000000, 3, null);
      blobClient.uploadFromFile(tmpFile.getAbsolutePath(), parallelTransferOptions, makeHeader(fileName), makeMetaData(user, userUpload, pii), null, null, null);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public List<BlobMetaData> getBlobMetadata(String containerName, Boolean withSnapshots) {
    ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
    BlobListDetails details = new BlobListDetails();
    details.setRetrieveMetadata(true);
    details.setRetrieveSnapshots(withSnapshots);
    listBlobsOptions.setDetails(details);
    PagedIterable<BlobItem> segment = getBlobContainerClient(containerName)
      .listBlobs(listBlobsOptions, null);
    if (segment.stream() != null) {
      return segment.stream().map(
        x -> new BlobMetaData(
          x.getName(),
          x.getSnapshot(),
          buildUrlForBlob(containerName, x.getName(), x.getSnapshot()),
          x.getProperties().getContentLength(),
          containerName,
          x.getProperties().getLastModified(),
          x.getMetadata()
        )).filter(item -> !item.fileName.startsWith("adfpolybaserejectedrows"))
        .collect(Collectors.toList());
    }

    return Collections.emptyList();
  }

  @Override
  public Set<String> getContainerNames() {
    return makeBlobServiceClient().listBlobContainers().stream().map(BlobContainerItem::getName).collect(Collectors.toSet());
  }

  @Override
  public byte[] getBlob(String containerName, String blobName) {
    return downloadBlob(makeBlobServiceClient().getBlobContainerClient(containerName).getBlobClient(blobName));
  }

  @Override
  public byte[] getBlob(String containerName, String blobName, String snapshot) {
    return downloadBlob(makeBlobServiceClient().getBlobContainerClient(containerName).getBlobClient(blobName, snapshot));
  }

  private byte[] downloadBlob(BlobClient blobClient) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    blobClient.download(outputStream);
    return outputStream.toByteArray();
  }

  @Override
  public void makeSnapshot(String containerName, String blobName) {
    getBlobContainerClient(containerName).getBlobClient(blobName).createSnapshot();
  }


  @Override
  public void delete(String containerName, String blobName, String snapshot) {
    log.info("Deleting blob snapshot: {} {}", blobName, snapshot);
    getBlobContainerClient(containerName).getBlobClient(blobName, snapshot).delete();
  }

  @Override
  public void delete(String containerName, String blobNamePattern) {
    for (BlobMetaData b : getBlobMetadata(containerName, true)) {
      if (b.getFileName().contains(blobNamePattern)) {
        if (b.getSnapshot() != null) {
          log.info("Deleting blob snapshot: {} {}", b.getFileName(), b.getSnapshot());
          getBlobContainerClient(containerName).getBlobClient(b.getFileName(), b.getSnapshot()).delete();
        }
      }
    }

    for (BlobMetaData b : getBlobMetadata(containerName, true)) {
      if (b.getFileName().contains(blobNamePattern)) {
        log.info("Deleting blob: {}", b.getFileName());
        getBlobContainerClient(containerName).getBlobClient(b.getFileName()).delete();
      }
    }
  }

  @Override
  public Boolean isPublic(String containerName) {
    PublicAccessType blobAccessType = getBlobContainerClient(containerName).getAccessPolicy().getBlobAccessType();
    return (blobAccessType == PublicAccessType.BLOB || blobAccessType == PublicAccessType.CONTAINER);
  }

  private String buildUrlForBlob(String containerName, String blobName, String snapshot) {
    String blobUrl = buildStorageAccountBaseUrl() + containerName + "/" + blobName;
    return (snapshot != null) ? blobUrl + "?snapshot=" + snapshot : blobUrl;
  }

  private BlobContainerClient getBlobContainerClient(String containerName) {
    return makeBlobServiceClient().getBlobContainerClient(containerName);
  }

  private BlobServiceClient makeBlobServiceClient() {
    String connectionString = "DefaultEndpointsProtocol=https;AccountName=" + accountName + ";AccountKey=" + accountKey + ";EndpointSuffix=core.windows.net";
    return new BlobServiceClientBuilder().connectionString(connectionString).buildClient();
  }

  private String buildStorageAccountBaseUrl() {
    return String.format("https://%s.blob.core.windows.net/", accountName);
  }

  private HashMap<String, String> makeMetaData(String uploadedBy, Boolean userUpload, Boolean pii) {
    HashMap<String, String> metadata = new HashMap<String, String>();
    metadata.put("uploaded_by", uploadedBy);
    metadata.put("user_upload", userUpload.toString());
    metadata.put("pii", pii.toString());
    return metadata;
  }

  private BlobHttpHeaders makeHeader(String fileName) {
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

    BlobHttpHeaders headers = new BlobHttpHeaders();
    headers.setContentType(contentType);
    return headers;
  }
}
