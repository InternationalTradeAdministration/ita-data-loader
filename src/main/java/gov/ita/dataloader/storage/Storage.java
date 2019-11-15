package gov.ita.dataloader.storage;

import java.util.List;
import java.util.Set;

public interface Storage {
  void createContainer(String containerName);

  void save(String fileName, byte[] fileContent, String user, String containerName, Boolean userUpload);

  String getListBlobsUrl(String containerName);

  List<BlobMetaData> getBlobMetadata(String containerName);

  Set<String> getContainerNames();

  byte[] getBlob(String containerName, String blobName);

  void makeSnapshot(String containerName, String blobName);

  void delete(String containerName, String blobPattern);
}
