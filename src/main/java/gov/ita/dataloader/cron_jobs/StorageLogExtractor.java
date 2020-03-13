package gov.ita.dataloader.cron_jobs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.ita.dataloader.business_unit.BusinessUnitService;
import gov.ita.dataloader.storage.Storage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class StorageLogExtractor {

  private Storage storage;
  private BusinessUnitService businessUnitService;
  private ObjectMapper objectMapper;

  public StorageLogExtractor(Storage storage, BusinessUnitService businessUnitService, ObjectMapper objectMapper) {
    this.storage = storage;
    this.businessUnitService = businessUnitService;
    this.objectMapper = objectMapper;
  }

  @Scheduled(cron = "0 2 0 * * ?")
  public void extractStorageLog() throws Exception {
    log.info("Extracting blob storage metadata");

    businessUnitService.getStorageContainers().forEach(container -> {
      byte[] usageStats = null;
      try {
        usageStats = objectMapper.writeValueAsBytes(storage.getBlobMetadata(container, true));
      } catch (JsonProcessingException e) {
        e.printStackTrace();
      }
      storage.save(String.format("storage-log/%s.json", container), usageStats, null, "dataloader", false, false);
    });

  }
}
