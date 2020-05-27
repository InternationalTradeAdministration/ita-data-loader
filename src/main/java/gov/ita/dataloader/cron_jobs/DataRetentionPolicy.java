package gov.ita.dataloader.cron_jobs;

import gov.ita.dataloader.ingest.configuration.BusinessUnitService;
import gov.ita.dataloader.storage.BlobMetaData;
import gov.ita.dataloader.storage.Storage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

import static java.time.temporal.ChronoUnit.DAYS;

@Slf4j
@Service
public class DataRetentionPolicy {

  private final Storage storage;
  private final BusinessUnitService businessUnitService;
  private final DateHelper dateHelper;
  private static final Integer PII_DATA_RETENTION_YEARS = 6;
  private static final Integer NON_PII_DATA_RETENTION_MONTHS = 13;

  public DataRetentionPolicy(Storage storage, BusinessUnitService businessUnitService, DateHelper dateHelper) {
    this.storage = storage;
    this.businessUnitService = businessUnitService;
    this.dateHelper = dateHelper;
  }

  @Scheduled(cron = "0 1 0 * * ?")
  public void purgeExpiredData() throws Exception {
    log.info("Executing data retention policy for container");
    businessUnitService.getStorageContainers().forEach(container -> {
      List<BlobMetaData> blobMetadata = storage.getBlobMetadata(container, true);
      blobMetadata.forEach(b -> {
        if (b.getSnapshot() != null && b.getMetadata() != null && b.getMetadata().get("pii") != null) {
          long ageInDays = DAYS.between(b.getUploadedAt(), dateHelper.now());
          if (b.getMetadata().get("pii").equals("true") && ageInDays > (PII_DATA_RETENTION_YEARS * 365)) {
            log.info("Deleting file, {} {} is older than 6 years", b.getFileName(), b.getSnapshot());
            storage.delete(b.getContainerName(), b.getFileName(), b.getSnapshot());
          } else if (b.getMetadata().get("pii").equals("false") && ageInDays > (NON_PII_DATA_RETENTION_MONTHS * 31)) {
            log.info("Deleting file, {} {} is older than 13 months", b.getFileName(), b.getSnapshot());
            storage.delete(b.getContainerName(), b.getFileName(), b.getSnapshot());
          }
        }
      });
    });
  }
}
