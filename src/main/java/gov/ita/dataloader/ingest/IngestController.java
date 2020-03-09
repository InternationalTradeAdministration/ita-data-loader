package gov.ita.dataloader.ingest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.ita.dataloader.ingest.configuration.DataloaderConfig;
import gov.ita.dataloader.security.AuthenticationFacade;
import gov.ita.dataloader.storage.BlobMetaData;
import gov.ita.dataloader.storage.Storage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@RestController
public class IngestController {

  private Storage storage;
  private AutomatedIngestProcessor automatedIngestProcessor;
  private AuthenticationFacade authenticationFacade;
  private ObjectMapper objectMapper;
  private TranslationProcessor translationProcessor;

  public IngestController(Storage storage,
                          AutomatedIngestProcessor automatedIngestProcessor,
                          AuthenticationFacade authenticationFacade,
                          ObjectMapper objectMapper,
                          TranslationProcessor translationProcessor) {
    this.storage = storage;
    this.automatedIngestProcessor = automatedIngestProcessor;
    this.authenticationFacade = authenticationFacade;
    this.objectMapper = objectMapper;
    this.translationProcessor = translationProcessor;
  }

  @GetMapping(value = "/api/configuration", produces = MediaType.APPLICATION_JSON_VALUE)
  private DataloaderConfig getDataSetConfigs(@RequestParam("containerName") String containerName) {
    return getDataloaderConfig(containerName);
  }

  //  @PreAuthorize("hasRole('ROLE_TSI_AllUsers')")
  @GetMapping("/api/ingest")
  public void startIngestProcess(@RequestParam("containerName") String containerName) {
    automatedIngestProcessor.process(
      getDataloaderConfig(containerName).getDataSetConfigs(),
      containerName,
      authenticationFacade.getUserName(),
      5000);
  }

  //  @PreAuthorize("hasRole('ROLE_TSI_AllUsers')")
  @PutMapping("/api/file")
  public void saveFile(@RequestParam("file") MultipartFile file,
                       @RequestParam("containerName") String containerName,
                       @RequestParam("pii") Boolean pii) throws IOException {
    storage.save(file.getOriginalFilename(), file.getBytes(), authenticationFacade.getUserName(), containerName, true, pii);
    storage.makeSnapshot(containerName, file.getOriginalFilename());
    translationProcessor.initProcessing(containerName, file.getOriginalFilename(), file.getBytes(), authenticationFacade.getUserName());
  }

  //  @PreAuthorize("hasRole('ROLE_TSI_AllUsers')")
  @DeleteMapping("/api/file")
  public void deleteFile(@RequestParam("fileName") String fileName,
                         @RequestParam("containerName") String containerName,
                         @RequestParam("snapshot") String snapshot) {
    storage.delete(containerName, fileName, snapshot);
  }

  //  @PreAuthorize("hasRole('ROLE_TSI_AllUsers')")
  @GetMapping("/api/reprocess/file")
  public void reProcessFile(@RequestParam("containerName") String containerName, @RequestParam("fileName") String fileName) {
    translationProcessor.reProcess(containerName, fileName);
  }

  //  @PreAuthorize("hasRole('ROLE_TSI_AllUsers')")
  @PutMapping("/api/configuration")
  public void saveConfiguration(@RequestBody DataloaderConfig dataloaderConfig,
                                @RequestParam("containerName") String containerName) throws JsonProcessingException {
    byte[] dataSetConfigsJsonBytes = objectMapper.writeValueAsString(dataloaderConfig).getBytes();
    storage.save("configuration.json", dataSetConfigsJsonBytes, authenticationFacade.getUserName(), containerName, true, false);
    storage.makeSnapshot(containerName, "configuration.json");
  }

  @GetMapping("/api/storage-content-url")
  public String getStorageContentUrl(@RequestParam("containerName") String containerName) {
    return storage.getListBlobsUrl(containerName);
  }

  @GetMapping(value = "/api/container-metadata", produces = MediaType.APPLICATION_JSON_VALUE)
  public List<BlobMetaData> getStorageMetadata(@RequestParam("containerName") String containerName) {
    return storage.getBlobMetadata(containerName, true).stream()
      .filter(blobMetaData -> !blobMetaData.getFileName().equals("configuration.json"))
      .filter(blobMetaData -> !blobMetaData.getFileName().startsWith("translated"))
      .map(this::handleLegacyBlobMetadata)
      .collect(Collectors.toList());
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

  @GetMapping(value = "/api/{containerName}/{blobName}")
  public ResponseEntity<ByteArrayResource> downloadBlob(@PathVariable String containerName,
                                                        @PathVariable String blobName,
                                                        @RequestParam(name = "snapshot", defaultValue = "") String snapshot) {
    byte[] blob = (!snapshot.equals("")) ?
      storage.getBlob(containerName, blobName, snapshot) :
      storage.getBlob(containerName, blobName);
    ByteArrayResource resource = new ByteArrayResource(blob);

    return ResponseEntity.ok()
      .header(HttpHeaders.CONTENT_DISPOSITION,
        "attachment;filename=" + blobName)
      .contentType(MediaType.APPLICATION_PDF).contentLength(blob.length)
      .body(resource);
  }

  private DataloaderConfig getDataloaderConfig(String containerName) {
    try {
      byte[] blob = storage.getBlob(containerName, "configuration.json");
      return objectMapper.readValue(blob, DataloaderConfig.class);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return null;
  }
}
