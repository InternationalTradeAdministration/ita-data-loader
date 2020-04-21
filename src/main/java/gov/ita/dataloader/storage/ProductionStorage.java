package gov.ita.dataloader.storage;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.util.Configuration;
import com.azure.storage.blob.*;
import com.azure.storage.blob.models.*;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.microsoft.rest.v2.http.HttpPipeline;
import gov.ita.dataloader.HttpHelper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.azure.storage.blob.models.DeleteSnapshotsOptionType.INCLUDE;

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

  private String connectionString = "DefaultEndpointsProtocol=https;AccountName="+accountName+";AccountKey="+accountKey+";EndpointSuffix=core.windows.net";
  private BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(connectionString).buildClient();

  @Override
  public void createContainer(String containerName) {
    //log.info("Initializing container: {}", containerName);
    blobServiceClient.createBlobContainer(containerName);
    //makeContainerUrl(containerName).create(null, null, null).blockingGet();
  }

  @Override
  public void save(String fileName, byte[] fileContent, String user, String containerName, Boolean userUpload, Boolean pii) {
    try {
      if (user == null) user = accountName;

      // ContainerURL containerURL = makeContainerUrl(containerName);
      blobServiceClient.createBlobContainer(containerName);
      BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);

      //BlockBlobURL blobURL = containerURL.createBlockBlobURL(fileName);
      BlobClient blobClient = containerClient.getBlobClient(fileName);

      File tmpFile = File.createTempFile("tmpFile", ".tmp");
      OutputStream os = new FileOutputStream(tmpFile);
      os.write(fileContent);
      os.close();

      AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(Paths.get(tmpFile.getAbsolutePath()));
      blobClient.uploadFromFile(tmpFile.getAbsolutePath(), true);
      /*TransferManagerUploadToBlockBlobOptions options =
        new TransferManagerUploadToBlockBlobOptions(
          null,
          makeHeader(fileName),
          makeMetaData(user, userUpload, pii),
          null,
          10);
      TransferManager.uploadFileToBlockBlob(fileChannel, blobURL, 1000000, 20000000, options).blockingGet();*/
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

  // I don't see the need?
  @Override
  public List<BlobMetaData> getBlobMetadata(String containerName, Boolean withSnapshots) {
    ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
    BlobListDetails details = new BlobListDetails();
    details.setRetrieveMetadata(true);
    details.setRetrieveSnapshots(withSnapshots);
    listBlobsOptions.setDetails(details);
    PagedIterable<BlobItem> segment = makeContainerUrl(containerName)
      .listBlobs(listBlobsOptions, null); //.blockingGet().body().segment();
    if (segment != null && segment.stream() != null) {
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

  //This function already exists as listBlobContainers() in azure.storage.blob.BlobServiceClient
  //@Override
  public Set<String> getContainerNames() {
    /*return makeServiceURL().listContainersSegment(null, null)
      .blockingGet().body()
      .containerItems().stream().map(BlobContainerItem::name)
      .collect(Collectors.toSet());*/
    return blobServiceClient.listBlobContainers().stream().map(BlobContainerItem::getName).collect(Collectors.toSet());
  }

  @Override
  public byte[] getBlob(String containerName, String blobName) {
    return downloadBlob(blobServiceClient.getBlobContainerClient(containerName).getBlobClient(blobName));
    /*BlobURL blobURL = makeContainerUrl(containerName).createBlobURL(blobName);
    return downloadBlob(blobURL);*/
  }

  @Override
  public byte[] getBlob(String containerName, String blobName, String snapshot) {
    return downloadBlob(blobServiceClient.getBlobContainerClient(containerName).getBlobClient(blobName, snapshot));
    /*BlobURL blobURL = null;
    try {
      blobURL = (snapshot == null) ?
        makeContainerUrl(containerName).createBlobURL(blobName) :
        makeContainerUrl(containerName).createBlobURL(blobName).withSnapshot(snapshot);
    } catch (MalformedURLException | UnknownHostException e) {
      e.printStackTrace();
    }

    return downloadBlob(blobURL);*/
  }

  // BlobClient has download(OutputStream stream)
  private byte[] downloadBlob(BlobClient blobClient) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    blobClient.download(outputStream);
    /*blobURL
      .download()
      .blockingGet()
      .body(new ReliableDownloadOptions())
      .blockingForEach(b -> outputStream.write(b.array()));*/
    return outputStream.toByteArray();
  }

  // BlobClient has createSnapshot()
  @Override
  public void makeSnapshot(String containerName, String blobName) {
    blobServiceClient.createBlobContainer(containerName).getBlobClient(blobName).createSnapshot();
    //makeServiceURL().createContainerURL(containerName).createBlobURL(blobName).createSnapshot().blockingGet();
  }

  // Given a container and blobName, BlobContainerClient has a getBlobClient method which has a getSnapshotClient(String snapshot)
  // That BlobClient has a delete method to delete the blob and snapshots
  @Override
  public void delete(String containerName, String blobName, String snapshot) {
    blobServiceClient.createBlobContainer(containerName).getBlobClient(blobName, snapshot).delete();
    /*try {
      log.info("Deleting blob snapshot: {} {}", blobName, snapshot);
      makeServiceURL().createContainerURL(containerName)
        .createBlobURL(blobName).withSnapshot(snapshot)
        .delete().blockingGet();
    } catch (MalformedURLException | UnknownHostException e) {
      e.printStackTrace();
    }*/
  }

  // Given a container and blobName, BlobContainerClient has a getBlobClient method
  // That BlobClient has a delete method to delete the blob and snapshots
  @Override
  public void delete(String containerName, String blobName) {
    blobServiceClient.createBlobContainer(containerName).getBlobClient(blobName).delete();
    /*for (BlobMetaData b : getBlobMetadata(containerName, false)) {
      if (b.getFileName().contains(blobName)) {
        log.info("Deleting blob: {}", b.getFileName());
        makeServiceURL().createContainerURL(containerName).
          createBlobURL(b.getFileName())
          .delete(INCLUDE, null, null).blockingGet();
      }
    }*/
  }

  private String buildUrlForBlob(String containerName, String blobName, String snapshot) {
    String blobUrl = buildStorageAccountBaseUrl() + containerName + "/" + blobName;
    return (snapshot != null) ? blobUrl + "?snapshot=" +  snapshot : blobUrl;
  }


  // Can be removed. Replaced with containerClient
  private BlobContainerClient makeContainerUrl(String containerName) {
    return makeServiceURL().createBlobContainer(containerName);
    /*BlobServiceAsyncClient serviceURL = makeServiceURL();
    assert serviceURL != null;
    return serviceURL.createBlobContainer(containerName);*/
  }

  // Can be removed. Replaced with BlobServiceClient
  private BlobServiceClient makeServiceURL() {
    return blobServiceClient;
    /*try {
      StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
      HttpPipeline pipeline = BlobAsyncClient.createPipeline(credential, new PipelineOptions());
      URL url = new URL(buildStorageAccountBaseUrl());
      Configuration serviceClientConfig = new Configuration();
      return new BlobServiceAsyncClient(pipeline, url);
      //return new BlobServiceAsyncClient(url, pipeline);
    } catch (InvalidKeyException | MalformedURLException e) {
      e.printStackTrace();
    }
    return null;*/
  }

  private String buildStorageAccountBaseUrl() {
    return String.format("https://%s.blob.core.windows.net/", accountName);
  }

  private HashMap<String, String> makeMetaData(String uploadedBy, Boolean userUpload, Boolean pii) {
    //Metadata metadata = new Metadata();
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
