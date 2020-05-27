package gov.ita.dataloader.storage;

import java.util.List;
import java.util.Set;

public interface Storage {
  void createContainer(String containerName, Boolean isPublic);

  void save(String fileName, byte[] fileContent, String user, String containerName, Boolean userUpload, Boolean pii);

  List<BlobMetaData> getBlobMetadata(String containerName, Boolean withSnapshots);

  Set<String> getContainerNames();

  byte[] getBlob(String containerName, String blobName);

  byte[] getBlob(String containerName, String blobName, String snapshot);

  void makeSnapshot(String containerName, String blobName);

  void delete(String containerName, String blobPattern, String snapshot);

  void delete(String containerName, String blobPattern);

  Boolean isPublic(String containerName);
}
