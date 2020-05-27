package gov.ita.dataloader.storage;

import gov.ita.dataloader.ingest.BlobDownloadRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping(value = "/api/storage", produces = MediaType.APPLICATION_JSON_VALUE)
public class StorageController {

  @Autowired
  private Storage storage;

  @GetMapping("/container/blobs")
  public List<BlobMetaData> getStorageMetadata(@RequestParam("containerName") String containerName) {
    return storage.getBlobMetadata(containerName, true).stream()
      .map(this::handleLegacyBlobMetadata)
      .collect(Collectors.toList());
  }

  @DeleteMapping("/file")
  public void deleteFile(@RequestParam("fileName") String fileName,
                         @RequestParam("containerName") String containerName,
                         @RequestParam(name = "snapshot", defaultValue = "") String snapshot) {
    if (snapshot.equals("")) {
      storage.delete(containerName, fileName);
    } else {
      storage.delete(containerName, fileName, snapshot);
    }
  }

  @PostMapping("/download")
  public ResponseEntity<Resource> downloadBlob(@RequestBody BlobDownloadRequest blobDownloadRequest) {
    String containerName = blobDownloadRequest.getContainerName();
    String blobName = blobDownloadRequest.getBlobName();
    String snapshot = blobDownloadRequest.getSnapshot();
    byte[] blob = (blobDownloadRequest.getSnapshot() != null) ?
      storage.getBlob(containerName, blobName, snapshot) :
      storage.getBlob(containerName, blobName);
    ByteArrayResource resource = new ByteArrayResource(blob);

    return ResponseEntity.ok()
      .header(HttpHeaders.CONTENT_DISPOSITION,
        "attachment;filename=\"" + blobName + "\"")
      .contentType(MediaType.APPLICATION_OCTET_STREAM).contentLength(blob.length)
      .body(resource);
  }

  @GetMapping("/container/public")
  Boolean isPublic(@RequestParam String containerName) {
    return storage.isPublic(containerName);
  }

  private BlobMetaData handleLegacyBlobMetadata(BlobMetaData blobMetaData) {
    Map<String, String> metadata = blobMetaData.getMetadata();
    if (metadata == null) {
      metadata = new HashMap<>();
      metadata.put("uploaded_by", "---");
      metadata.put("user_upload", "true");
      metadata.put("pii", "false");
    }

    if (!metadata.containsKey("uploaded_by")) metadata.put("uploaded_by", "---");
    if (!metadata.containsKey("user_upload")) metadata.put("user_upload", "true");
    if (!metadata.containsKey("pii")) metadata.put("pii", "false");
    blobMetaData.setMetadata(metadata);
    return blobMetaData;
  }
}
