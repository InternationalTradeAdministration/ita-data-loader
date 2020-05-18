package gov.ita.dataloader.ingest;

import gov.ita.dataloader.storage.BlobMetaData;
import gov.ita.dataloader.storage.Storage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Slf4j
@RestController
public class TranslationController {

  private Storage storage;
  private TranslationProcessor translationProcessor;

  public TranslationController(Storage storage, TranslationProcessor translationProcessor) {
    this.storage = storage;
    this.translationProcessor = translationProcessor;
  }

  @GetMapping("/api/translate")
  public String translate(@RequestParam String containerName) {
    List<BlobMetaData> blobMetadata = storage.getBlobMetadata(containerName, false);
    blobMetadata.forEach(b -> {
      if (translationProcessor.hasTranslator(containerName, b.getFileName())) {
        log.info("Translating {} {}", containerName, b.getFileName());
        byte[] blob = storage.getBlob(containerName, b.getFileName());
        translationProcessor.process(containerName, b.getFileName(), blob);
      }
    });
    return "done";
  }
}
