package gov.ita.dataloader.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.ita.dataloader.ingest.configuration.BusinessUnit;
import gov.ita.dataloader.ingest.configuration.DataloaderAdminConfiguration;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static gov.ita.dataloader.ResourceHelper.getResourceAsString;

@Service
public class StorageInitializer {

  @Autowired
  private Storage storage;

  @Autowired
  private ObjectMapper objectMapper;

  public void init() throws IOException {
    Map<String, ContainerConfig> newContainers = new HashMap<>();
    newContainers.put("dataloader", new ContainerConfig(true, false));
    newContainers.put("demo", new ContainerConfig(false, false));
    newContainers.put("fta-tariff-rates", new ContainerConfig(false, true));
    newContainers.put("i92", new ContainerConfig(false, false));
    newContainers.put("industry-data", new ContainerConfig(false, false));
    newContainers.put("otexa", new ContainerConfig(false, false));
    newContainers.put("select-usa", new ContainerConfig(true, false));
    newContainers.put("siat", new ContainerConfig(false, false));
    newContainers.put("sima", new ContainerConfig(false, false));
    newContainers.put("taxonomy", new ContainerConfig(false, true));

    Set<String> existingContainers = storage.getContainerNames();

    //Update container configurations for those that don't exist; once they exist, users control the configuration content.
    for (String containerName : newContainers.keySet()) {
      if (!existingContainers.contains(containerName)) {
        storage.createContainer(containerName, newContainers.get(containerName).isPublic);

        // Storage containers that have to be initialized with a configuration.json file
        if (newContainers.get(containerName).hasAutomatedConfig) {
          storage.save("configuration.json", getContainerConfiguration(containerName), null, containerName, false, false);
        }
      }
    }

    //Add new containers to the admin panel
    DataloaderAdminConfiguration dataloaderAdminConfiguration = getDataloaderAdminConfig();
    newContainers.keySet().forEach(containerName -> {
      if (!dataloaderAdminConfiguration.getBusinessUnits().stream().anyMatch(b -> b.getContainerName().equals(containerName))) {
        dataloaderAdminConfiguration.getBusinessUnits().add(new BusinessUnit(containerName, containerName, Collections.emptyList()));
      }
    });
    saveDataloaderAdminConfig(dataloaderAdminConfiguration);
  }

  private DataloaderAdminConfiguration getDataloaderAdminConfig() throws IOException {
    byte[] dataloaderConfig = storage.getBlob("dataloader", "configuration.json");
    return objectMapper.readValue(dataloaderConfig, DataloaderAdminConfiguration.class);
  }

  private void saveDataloaderAdminConfig(DataloaderAdminConfiguration dataloaderAdminConfiguration) throws JsonProcessingException {
    byte[] updatedDataloaderConfig = objectMapper.writeValueAsBytes(dataloaderAdminConfiguration);
    storage.save("configuration.json", updatedDataloaderConfig, null, "dataloader", false, false);
  }

  private byte[] getContainerConfiguration(String containerName) {
    return getResourceAsString("/fixtures/" + containerName + ".json").getBytes();
  }

  @Data
  @AllArgsConstructor
  class ContainerConfig {
    Boolean hasAutomatedConfig;
    Boolean isPublic;
  }
}
