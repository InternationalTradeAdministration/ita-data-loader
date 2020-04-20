package gov.ita.dataloader.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import gov.ita.dataloader.ingest.configuration.BusinessUnit;
import gov.ita.dataloader.ingest.configuration.DataloaderAdminConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

import static gov.ita.dataloader.ResourceHelper.getResourceAsString;

@Service
@Slf4j
public class StorageInitializer {

  @Autowired
  private Storage storage;

  @Autowired
  private ObjectMapper objectMapper;

  public void init() throws IOException {
    Map<String, Boolean> newContainers = new HashMap<>();
    newContainers.put("dataloader", true);
    newContainers.put("demo", false);
    newContainers.put("fta-tariff-rates", false);
    newContainers.put("i92", false);
    newContainers.put("industry-data", false);
    newContainers.put("otexa", false);
    newContainers.put("select-usa", true);
    newContainers.put("siat", false);
    newContainers.put("sima", false);

    Set<String> existingContainers = storage.getContainerNames();

    //Update container configurations for those that don't exist; once they exist, users control the configuration content.
    for (String containerName : newContainers.keySet()) {
      log.info("Initializing container: {}", containerName);
      if (!existingContainers.contains(containerName)) {
        storage.createContainer(containerName);

        // If a storage container needs to be initialized with a configuration.json file
        if (newContainers.get(containerName)) {
          String path = "/fixtures/" + containerName + ".json";
          storage.save("configuration.json", getResourceAsString(path).getBytes(), null, containerName, false, false);
        }
      }
    }

    //Add new containers to the admin panel
    byte[] dataloaderConfig = storage.getBlob("dataloader", "configuration.json");
    DataloaderAdminConfiguration dataloaderAdminConfiguration = objectMapper.readValue(dataloaderConfig, DataloaderAdminConfiguration.class);
    newContainers.keySet().forEach(containerName -> {
      if (!dataloaderAdminConfiguration.getBusinessUnits().stream().anyMatch(b -> b.getContainerName().equals(containerName))) {
        dataloaderAdminConfiguration.getBusinessUnits().add(new BusinessUnit(containerName, containerName, Collections.emptyList()));
      }
    });
    byte[] updatedDataloaderConfig = objectMapper.writeValueAsBytes(dataloaderAdminConfiguration);
    storage.save("configuration.json", updatedDataloaderConfig, null, "dataloader", false, false);
  }
}
