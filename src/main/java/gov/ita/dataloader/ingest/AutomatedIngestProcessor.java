package gov.ita.dataloader.ingest;

import gov.ita.dataloader.HttpHelper;
import gov.ita.dataloader.data_factory.DataFactoryGateway;
import gov.ita.dataloader.ingest.configuration.DataSetConfig;
import gov.ita.dataloader.ingest.configuration.ReplaceValue;
import gov.ita.dataloader.storage.Storage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


@Slf4j
@Service
public class AutomatedIngestProcessor {

  private ZipFileExtractor zipFileExtractor;
  private HttpHelper httpHelper;
  private TranslationProcessor translationProcessor;
  private Storage storage;
  private DataFactoryGateway dataFactoryGateway;
  private Map<String, List<LogItem>> logItems = new HashMap<>();
  private Map<String, Boolean> stopProcessing = new HashMap<>();

  public AutomatedIngestProcessor(ZipFileExtractor zipFileExtractor,
                                  HttpHelper httpHelper,
                                  TranslationProcessor translationProcessor,
                                  Storage storage,
                                  DataFactoryGateway dataFactoryGateway) {
    this.zipFileExtractor = zipFileExtractor;
    this.httpHelper = httpHelper;
    this.translationProcessor = translationProcessor;
    this.storage = storage;
    this.dataFactoryGateway = dataFactoryGateway;
  }

  @Async
  public CompletableFuture<String> process(List<DataSetConfig> dataSourceConfigs, String containerName, String userName, long sleepMs) {
    log.info("Starting ingest process for container: {}", containerName);
    List<DataSetConfig> enabledConfigs = dataSourceConfigs.stream().filter(DataSetConfig::isEnabled).collect(Collectors.toList());

    int totalApiCalls = enabledConfigs.size();
    int completedApiCalls = 0;

    byte[] fileBytes;
    try {
      for (DataSetConfig dsc : enabledConfigs) {
        log.info("Importing file {} from {} to container {}", dsc.getFileName(), dsc.getUrl(), containerName);

        updateLog(containerName, String.format("Retrieving data from: %s", dsc.getUrl()));
        fileBytes = httpHelper.getBytes(dsc.getUrl());

        processDataSource(dsc.getFileName(), fileBytes, dsc.getReplaceValues(), null, containerName, userName);

        if (dsc.getZipFileConfigs() != null) {
          updateLog(containerName, String.format("Processing zip file: %s", dsc.getUrl()));
          Map<String, ByteArrayOutputStream> fileMap;
          try {
            fileMap = zipFileExtractor.extract(fileBytes);
          } catch (IOException e) {
            e.printStackTrace();
            throw new AutomatedIngestProcessorException(
              String.format("%s Could not extract zip file from %s", LocalDateTime.now(), dsc.getUrl()));
          }

          assert fileMap != null;
          dsc.getZipFileConfigs().forEach(zipFileConfig -> {
            String fileNameKey = fileMap.keySet().stream()
              .filter(fileName -> fileName.matches(zipFileConfig.getOriginalFileName()))
              .findFirst().get();
            processDataSource(
              zipFileConfig.getDestinationFileName(),
              fileMap.get(fileNameKey).toByteArray(),
              zipFileConfig.getReplaceValues(),
              zipFileConfig.getSkipLineCount(),
              containerName,
              userName);
          });
        }

        completedApiCalls++;

        if (stopProcessing.getOrDefault(containerName, false)) {
          log.info("Ingest process cancelled for container: {}", containerName);
          updateLog(containerName, "Process cancelled");
          return CompletableFuture.completedFuture("cancelled");
        }

        try {
          Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
          updateLog(containerName, String.format("Error: %s", e.getMessage()));
          return CompletableFuture.completedFuture("error");
        }
      }
    } catch (Exception e) {
      updateLog(containerName, String.format("Error: %s", e.getMessage()));
      return CompletableFuture.completedFuture("error");
    } finally {
      log.info("Ingest process complete for container: {}", containerName);
      updateLog(containerName, String.format("Process complete; ingested (%s of %s)", completedApiCalls, totalApiCalls));
      stopProcessing.put(containerName, false);
      dataFactoryGateway.runPipeline(containerName, "");
    }

    return CompletableFuture.completedFuture("complete");
  }

  private byte[] replace(byte[] fileBytes, String replace, String with) {
    return new String(fileBytes).replaceAll(replace, with).getBytes();
  }

  private void processDataSource(String fileName,
                                 byte[] fileBytes,
                                 List<ReplaceValue> replaceValues,
                                 Integer skipLineCount,
                                 String containerName,
                                 String userName) {
    if (skipLineCount != null && skipLineCount > 0) {
      fileBytes = skipLines(fileBytes, skipLineCount);
    }

    if (replaceValues != null) {
      for (ReplaceValue rv : replaceValues) {
        fileBytes = replace(fileBytes, rv.getReplaceThis(), rv.getWithThis());
      }
    }

    updateLog(containerName, String.format("Saving %s", fileName));
    storage.save(fileName, fileBytes, userName, containerName, false, false);
    storage.makeSnapshot(containerName, fileName);
    translationProcessor.process(containerName, fileName, fileBytes);
  }

  private byte[] skipLines(byte[] fileBytes, Integer skipLineCount) {
    InputStream is;
    BufferedReader reader;
    BufferedWriter writer;

    is = new ByteArrayInputStream(fileBytes);
    StringWriter out = new StringWriter();
    writer = new BufferedWriter(out);
    reader = new BufferedReader(new InputStreamReader(is));
    String temp;

    try {
      for (int i = 0; i < skipLineCount; i++)
        reader.readLine();

      while ((temp = reader.readLine()) != null) {
        writer.write(temp);
        writer.newLine();
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        writer.flush();
        is.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    return out.toString().getBytes();
  }

  public List<LogItem> getLog(String containerName) {
    return logItems.get(containerName);
  }

  public void clearLog(String containerName) {
    if (logItems.get(containerName) != null) logItems.get(containerName).clear();
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

  public void stopProcessing(String containerName) {
    stopProcessing.put(containerName, true);
  }
}
