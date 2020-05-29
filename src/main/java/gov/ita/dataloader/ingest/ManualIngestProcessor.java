package gov.ita.dataloader.ingest;

import gov.ita.dataloader.data_factory.DataFactoryGateway;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Service
public class ManualIngestProcessor {

  private final TranslationProcessor translationProcessor;
  private final DataFactoryGateway dataFactoryGateway;
  private Map<String, List<LogItem>> logItems = new HashMap<>();

  public ManualIngestProcessor(TranslationProcessor translationProcessor, DataFactoryGateway dataFactoryGateway) {
    this.translationProcessor = translationProcessor;
    this.dataFactoryGateway = dataFactoryGateway;
  }

  @Async
  public CompletableFuture<String> process(String containerName, String fileName, byte[] fileBytes) {
    if (translationProcessor.hasTranslator(containerName, fileName)) {
      updateLog(containerName, String.format("Translating file: %s", fileName));
      translationProcessor.process(containerName, fileName, fileBytes);
    }
    dataFactoryGateway.runPipeline(containerName);
    updateLog(containerName, String.format("Completed uploading file: %s", fileName));
    return CompletableFuture.completedFuture("complete");
  }

  private void updateLog(String containerName, String message) {
    List<LogItem> tmpLogItems = this.logItems.get(containerName);
    if (tmpLogItems != null) {
      tmpLogItems.add(new LogItem(message));
    } else {
      tmpLogItems = new ArrayList<>();
      tmpLogItems.add(new LogItem(message));
      this.logItems.put(containerName, tmpLogItems);
    }
  }

  public List<LogItem> getLog(String containerName) {
    return logItems.get(containerName);
  }

  public void clearLog(String containerName) {
    if (logItems.get(containerName) != null) logItems.get(containerName).clear();
  }
}
