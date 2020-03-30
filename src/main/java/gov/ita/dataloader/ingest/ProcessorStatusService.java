package gov.ita.dataloader.ingest;

import lombok.Data;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
@Service
public class ProcessorStatusService {
  private Map<String, ManualIngestProcessorStatus> ingestProcessorStatusMap = new HashMap<>();
  private Map<String, Map<String, ManualIngestTranslationStatus>> ingestTranslationStatusMap = new HashMap<>();

  public void updateIngestProcessorStatus(String containerName, ManualIngestProcessorStatus manualIngestProcessorStatus) {
    ingestProcessorStatusMap.put(containerName, manualIngestProcessorStatus);
  }

  public void updateTranslationProcessorStatus(String containerName, String fileName, ManualIngestTranslationStatus ingestProcessorStatus) {
    Map<String, ManualIngestTranslationStatus> ingestTranslationStatusFileMap = ingestTranslationStatusMap.computeIfAbsent(containerName+"#"+fileName, k -> new HashMap<>());
    ingestTranslationStatusFileMap.put(fileName, ingestProcessorStatus);
  }

  public boolean isIngesting(String containerName, String fileName) {
    return ingestProcessorStatusMap.get(containerName+"#"+fileName).isIngesting();
  }

  public boolean isIngesting(String containerName) {
    List<String> collect = ingestProcessorStatusMap.keySet().stream()
      .filter(k -> k.contains(containerName) && ingestProcessorStatusMap.get(k).isIngesting())
      .collect(Collectors.toList());
    return collect.size() > 0;
  }

  public boolean isProcessing(String containerName, String fileName) {
    Map<String, ManualIngestTranslationStatus> stringIngestTranslationStatusMap = ingestTranslationStatusMap.get(containerName+"#"+fileName);
    if (stringIngestTranslationStatusMap != null) {
      ManualIngestTranslationStatus manualIngestTranslationStatus = stringIngestTranslationStatusMap.get(fileName);
      if (manualIngestTranslationStatus != null) {
        return manualIngestTranslationStatus.isProcessing();
      } else {
        return false;
      }
    } else {
      return false;
    }
  }
}
