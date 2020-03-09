package gov.ita.dataloader.ingest;

import gov.ita.dataloader.HttpHelper;
import gov.ita.dataloader.ingest.configuration.DataSetConfig;
import gov.ita.dataloader.ingest.configuration.ReplaceValue;
import gov.ita.dataloader.storage.Storage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
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
  private ProcessorStatusService processorStatusService;
  private Storage storage;

  public AutomatedIngestProcessor(ZipFileExtractor zipFileExtractor,
                                  HttpHelper httpHelper,
                                  TranslationProcessor translationProcessor,
                                  ProcessorStatusService processorStatusService,
                                  Storage storage) {
    this.zipFileExtractor = zipFileExtractor;
    this.httpHelper = httpHelper;
    this.translationProcessor = translationProcessor;
    this.processorStatusService = processorStatusService;
    this.storage = storage;
  }

  @Async
  public CompletableFuture<Void> process(List<DataSetConfig> dataSourceConfigs, String containerName, String userName, long sleepMs) {
    log.info("Starting ingest process for container: {}", containerName);
    List<DataSetConfig> enabledConfigs = dataSourceConfigs.stream().filter(DataSetConfig::isEnabled).collect(Collectors.toList());
    ManualIngestProcessorStatus manualIngestProcessorStatus = initializeStatus(containerName, enabledConfigs.size());

    if (processorStatusService.isIngesting(containerName)) return new CompletableFuture<>();

    byte[] fileBytes;
    try {
      for (DataSetConfig dsc : enabledConfigs) {
        log.info("Importing file {} from {} to container {}", dsc.getFileName(), dsc.getUrl(), containerName);

        fileBytes = httpHelper.getBytes(dsc.getUrl());

        processDataSource(dsc.getFileName(), fileBytes, dsc.getReplaceValues(), null, containerName, userName);

        if (dsc.getZipFileConfigs() != null) {
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
              containerName, userName);
          });
        }

        updateStatus(dsc, manualIngestProcessorStatus);

        try {
          Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
          manualIngestProcessorStatus.getLog().add(new LogItem(e.getMessage()));
        }
      }
    } catch (Exception e) {
      manualIngestProcessorStatus.getLog().add(new LogItem(e.getMessage()));
    } finally {
      manualIngestProcessorStatus.getLog().add(new LogItem("Ingest process complete"));
      manualIngestProcessorStatus.setIngesting(false);
      log.info("Ingest process complete for container: {}", containerName);
    }

    return new CompletableFuture<>();
  }

  private ManualIngestProcessorStatus initializeStatus(String containerName, int totalApiCalls) {
    ManualIngestProcessorStatus ips = new ManualIngestProcessorStatus(totalApiCalls, 0, false, new ArrayList<>());
    processorStatusService.updateIngestProcessorStatus(containerName, ips);
    return ips;
  }

  private void updateStatus(DataSetConfig dataSetConfig, ManualIngestProcessorStatus manualIngestProcessorStatus) {
    manualIngestProcessorStatus.setDatasetsCompleted(manualIngestProcessorStatus.getDatasetsCompleted() + 1);
    manualIngestProcessorStatus.getLog().add(new LogItem(String.format("Completed ingest of URL: %s", dataSetConfig.getUrl())));
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

    storage.save(fileName, fileBytes, userName, containerName, false, false);
    storage.makeSnapshot(containerName, fileName);
    translationProcessor.initProcessing(containerName, fileName, fileBytes, userName);
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

}
