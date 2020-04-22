package gov.ita.dataloader;

import gov.ita.dataloader.storage.StorageInitializer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Slf4j
@Component
public class DataloaderInitializer implements ApplicationListener<ContextRefreshedEvent> {

  private final StorageInitializer storageInitializer;

  public DataloaderInitializer(StorageInitializer storageInitializer) {
    this.storageInitializer = storageInitializer;
  }

  @Override
  public void onApplicationEvent(ContextRefreshedEvent event) {
    log.info("Initializing storage");
    try {
      storageInitializer.init();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
