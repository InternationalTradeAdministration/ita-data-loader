package gov.ita.dataloader.ingest;

import gov.ita.dataloader.ingest.translators.Translator;
import gov.ita.dataloader.ingest.translators.TranslatorFactory;
import gov.ita.dataloader.ingest.translators.TranslatorType;
import gov.ita.dataloader.storage.Storage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
public class TranslationProcessor {

  private Storage storage;
  private TranslatorFactory translatorFactory;

  public TranslationProcessor(Storage storage, TranslatorFactory translatorFactory) {
    this.storage = storage;
    this.translatorFactory = translatorFactory;
  }

  public boolean hasTranslator(String containerName, String fileName) {
    String containerFileCompositeKey = containerName + "#" + fileName;
    return translatorFactory.getTranslator(containerFileCompositeKey) != null;
  }

  public CompletableFuture<String> process(String containerName, String fileName, byte[] fileBytes) {
    String containerFileCompositeKey = containerName + "#" + fileName;
    String fileRootName = "translated/" + fileName;

    Translator translator = translatorFactory.getTranslator(containerFileCompositeKey);
    if (translator != null) {
      log.info("Processing {}", containerFileCompositeKey);
      storage.delete(containerName, fileRootName);

      if (translator.type().equals(TranslatorType.CSV) && translator.pageSize() != -1) {
        int lineCount = countLines(fileBytes);
        int pageSize = translator.pageSize();
        if (pageSize == -1) pageSize = lineCount;
        int totalPages = (lineCount + pageSize - 1) / pageSize;
        int currentPage = 0;

        while (currentPage < totalPages) {
          log.info("Translating page {} of {} for {}", currentPage + 1, totalPages, containerFileCompositeKey);
          int offset = currentPage * pageSize;
          byte[] partitionedBytes = getFilePartition(fileBytes, offset, pageSize);
          byte[] translatedBytes = translator.translate(partitionedBytes);
          storage.save(fileRootName + "/" + UUID.randomUUID(), translatedBytes, "system", containerName, true, false);
          currentPage++;
        }
      } else {
        log.info("Translating {}", containerFileCompositeKey);
        byte[] translatedBytes = translator.translate(fileBytes);
        storage.save(fileRootName + "/" + UUID.randomUUID(), translatedBytes, "system", containerName, true, false);
      }
    }
    return CompletableFuture.completedFuture("complete");
  }

  private byte[] getFilePartition(byte[] bytes, int offset, int size) {
    BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(bytes)));
    StringWriter writer = new StringWriter();
    String temp = null;
    int i = 0;
    while (true) {
      try {
        if ((temp = reader.readLine()) == null) break;
      } catch (IOException e) {
        e.printStackTrace();
      }

      if ((i == 0) || (i > offset && (i - offset <= size))) {
        writer.write(temp);
        writer.append(System.getProperty("line.separator"));
      }
      i++;
    }

    return writer.toString().getBytes();
  }

  private int countLines(byte[] bytes) {
    InputStream is = new ByteArrayInputStream(bytes);
    byte[] c = new byte[1024];

    try {
      int readChars = is.read(c);
      if (readChars == -1) {
        return 0;
      }

      int count = 0;
      while (readChars == 1024) {
        for (int i = 0; i < 1024; ) {
          if (c[i++] == '\n') {
            ++count;
          }
        }
        readChars = is.read(c);
      }

      while (readChars != -1) {
        for (int i = 0; i < readChars; ++i) {
          if (c[i] == '\n') {
            ++count;
          }
        }
        readChars = is.read(c);
      }

      return count == 0 ? 1 : count;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return -1;
  }

}
