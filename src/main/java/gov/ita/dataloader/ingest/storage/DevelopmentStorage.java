package gov.ita.dataloader.ingest.storage;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.*;

@Slf4j
@Service
@Profile("development")
public class DevelopmentStorage implements Storage {

  private Map<String, Map<String, byte[]>> storageContent = new HashMap<>();

  @Override
  public void createContainer(String containerName) {

  }

  @Override
  public void save(String fileName, byte[] fileContent, String user, String containerName) {
    log.info("Saving blob: {}, {}, {}", fileName, containerName, user);

    Map<String, byte[]> containerContent = storageContent.get(containerName);
    if (containerContent == null)
      containerContent = new HashMap<>();

    containerContent.put(fileName, fileContent);
    storageContent.put(containerName, containerContent);
  }

  @Override
  public String getListBlobsUrl(String containerName) {
    return "http://cool.io";
  }

  @Override
  public List<BlobMetaData> getBlobMetadata(String containerName) {
    List<BlobMetaData> blobMetaDataList = new ArrayList<>();
    for (String container : storageContent.keySet()) {
      if (container.equals(containerName)) {
        Map<String, byte[]> containerContent = storageContent.get(container);
        for (String fileName : containerContent.keySet()) {
          BlobMetaData blobMetaData = new BlobMetaData(
            fileName,
            String.format("http://some-cloud-strage-url.com/%s/%s", containerName, fileName),
            123L,
            OffsetDateTime.now(),
            "TestUser@trade.gov");
          blobMetaDataList.add(blobMetaData);
        }
      }
    }

    return blobMetaDataList;
  }

  @Override
  public Set<String> getContainerNames() {
    return storageContent.keySet();
  }

  @Override
  public byte[] getBlob(String containerName, String blobName) {
    return storageContent.get(containerName).get(blobName);
  }
}