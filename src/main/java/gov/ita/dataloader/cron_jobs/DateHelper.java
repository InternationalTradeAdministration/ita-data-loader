package gov.ita.dataloader.cron_jobs;

import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;

@Service
public class DateHelper {

  public OffsetDateTime now() {
    return OffsetDateTime.now();
  }
}
