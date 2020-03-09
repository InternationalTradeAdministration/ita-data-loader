package gov.ita.dataloader.storage;

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
  public void save(String fileName, byte[] fileContent, String user, String containerName, Boolean userUpload, Boolean pii) {
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
  public List<BlobMetaData> getBlobMetadata(String containerName, Boolean withSnapshots) {
    List<BlobMetaData> blobMetaDataList = new ArrayList<>();

    Map<String, String> metadata = new HashMap<>();
    metadata.put("uploaded_by", "TestUser@trade.gov");

    for (String container : storageContent.keySet()) {
      if (container.equals(containerName)) {
        Map<String, byte[]> containerContent = storageContent.get(container);
        for (String fileName : containerContent.keySet()) {
          BlobMetaData blobMetaData = new BlobMetaData(
            fileName,
            null,
            String.format("http://some-cloud-strage-url.com/%s/%s", containerName, fileName),
            123L,
            containerName,
            OffsetDateTime.now(),
            metadata
          );
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

  @Override
  public byte[] getBlob(String containerName, String blobName, String snapshot) {
    return storageContent.get(containerName).get(blobName);
  }

  @Override
  public void makeSnapshot(String containerName, String blobName) {

  }

  @Override
  public void delete(String containerName, String blobPattern, String snapshot) {
    for (String container : storageContent.keySet()) {
      if (container.equals(containerName)) {
        Map<String, byte[]> containerContent = storageContent.get(container);
        List<String> fileNamesToRemove = new ArrayList<>();
        for (String fileName : containerContent.keySet()) {
          if (fileName.contains(blobPattern)) {
            fileNamesToRemove.add(fileName);
          }
        }

        for (String fileName : fileNamesToRemove) {
          log.info("Deleting blob: {}", fileName);
          containerContent.remove(fileName);
        }
      }
    }
  }
}
