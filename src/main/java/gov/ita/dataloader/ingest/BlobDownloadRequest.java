package gov.ita.dataloader.ingest;

import lombok.Data;

@Data
public class BlobDownloadRequest {
  String containerName;
  String blobName;
  String snapshot;
}
