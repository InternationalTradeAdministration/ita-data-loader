package gov.ita.dataloader.storage;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static gov.ita.dataloader.ResourceHelper.getResourceAsString;

@Service
@Slf4j
public class StorageInitializer {

  @Autowired
  private Storage storage;

  public void init() {
    List<String> newContainers = new ArrayList<>();
    newContainers.add("dataloader");
    newContainers.add("otexa");
    newContainers.add("select-usa");
    newContainers.add("fta-tariff-rates");
    newContainers.add("demo");
    newContainers.add("siat");

    Set<String> existingContainers = storage.getContainerNames();

    //Update container configurations for those that don't exist; once that exist, users control the configuration content.
    for (String containerName : newContainers) {
      log.info("Initializing container: {}", containerName);
      if (!existingContainers.contains(containerName)) {
        String path = "/fixtures/" + containerName + ".json";
        storage.createContainer(containerName);
        storage.save("configuration.json", getResourceAsString(path).getBytes(), null, containerName, false, false);
      }
    }
  }
}
