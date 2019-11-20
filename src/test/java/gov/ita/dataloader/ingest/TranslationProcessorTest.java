package gov.ita.dataloader.ingest;

import gov.ita.dataloader.TestHelpers;
import gov.ita.dataloader.ingest.translators.Translator;
import gov.ita.dataloader.ingest.translators.TranslatorFactory;
import gov.ita.dataloader.ingest.translators.TranslatorType;
import gov.ita.dataloader.storage.Storage;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TranslationProcessorTest {

  @Mock
  Storage storage;

  @Mock
  TranslatorFactory translatorFactory;

  @Mock
  Translator translator;

  private ProcessorStatusService processorStatusService = new ProcessorStatusService();

  private TestHelpers h = new TestHelpers();

  @Test
  public void usesPageSizeToPartitionFile() {
    byte[] FIRST_TRANSLATED_BYTES = "first".getBytes();
    byte[] SECOND_TRANSLATED_BYTES = "second".getBytes();
    byte[] THIRD_TRANSLATED_BYTES = "third".getBytes();

    byte[] wholeFile = "HEADER RECORD\r\nFIRST ROW\r\nSECOND ROW\r\nTHIRD ROW\r\nFOURTH ROW\r\nFIFTH ROW\r\n".getBytes();
    byte[] firstPartition = "HEADER RECORD\nFIRST ROW\nSECOND ROW\n".getBytes();
    byte[] secondPartition = "HEADER RECORD\nTHIRD ROW\nFOURTH ROW\n".getBytes();
    byte[] thirdPartition = "HEADER RECORD\nFIFTH ROW\n".getBytes();

    when(translatorFactory.getTranslator("some-container#some-file-name.csv")).thenReturn(translator);
    when(translator.pageSize()).thenReturn(2);
    when(translator.type()).thenReturn(TranslatorType.CSV);
    when(translator.translate(firstPartition)).thenReturn(FIRST_TRANSLATED_BYTES);
    when(translator.translate(secondPartition)).thenReturn(SECOND_TRANSLATED_BYTES);
    when(translator.translate(thirdPartition)).thenReturn(THIRD_TRANSLATED_BYTES);

    TranslationProcessor translationProcessor = new TranslationProcessor(storage, translatorFactory, processorStatusService);
    translationProcessor.saveAndProcess("some-container", "some-file-name.csv", wholeFile, "TestUser@gmail.com");

    verify(storage).delete("some-container", "translated/some-file-name.csv");
    verify(storage).save(anyString(), eq(FIRST_TRANSLATED_BYTES), eq("TestUser@gmail.com"), eq("some-container"), eq(true));
    verify(storage).save(anyString(), eq(SECOND_TRANSLATED_BYTES), eq("TestUser@gmail.com"), eq("some-container"), eq(true));
    verify(storage).save(anyString(), eq(THIRD_TRANSLATED_BYTES), eq("TestUser@gmail.com"), eq("some-container"), eq(true));
  }

  @Test
  public void skipsProcessingWhenAlreadyInProgress() {
    ManualIngestTranslationStatus status = new ManualIngestTranslationStatus("some-file-name.csv", 2, 1, Phase.CREATING_NEW_TRANSLATIONS);
    processorStatusService.updateTranslationProcessorStatus("some-container", "some-file-name.csv", status);

    byte[] FIRST_TRANSLATED_BYTES = "first".getBytes();
    byte[] wholeFile = "HEADER RECORD\r\nFIRST ROW\r\nSECOND ROW\r\nTHIRD ROW\r\nFOURTH ROW\r\nFIFTH ROW".getBytes();

    when(translatorFactory.getTranslator("some-container#some-file-name.csv")).thenReturn(translator);

    TranslationProcessor translationProcessor = new TranslationProcessor(storage, translatorFactory, processorStatusService);
    translationProcessor.saveAndProcess("some-container", "some-file-name.csv", wholeFile, "TestUser@gmail.com");

    verify(storage, never()).delete("some-container", "translated/some-file-name.csv");
    verify(storage, never()).save(anyString(), eq(FIRST_TRANSLATED_BYTES), eq("TestUser@gmail.com"), eq("some-container"), eq(true));
  }

  @Test
  public void savesUploadedFileAndMakesSnapshot() throws IOException {
    byte[] bytes = h.get("/fixtures/otexa/OTEXA_EXE_HTS.csv");
    when(translatorFactory.getTranslator("some-container#some-file-name.csv")).thenReturn(translator);
    when(translator.type()).thenReturn(TranslatorType.NONE);

    TranslationProcessor translationProcessor = new TranslationProcessor(storage, translatorFactory, processorStatusService);
    translationProcessor.saveAndProcess("some-container", "some-file-name.csv", bytes, "TestUser@gmail.com");

    verify(storage, times(1)).save("some-file-name.csv", bytes, "TestUser@gmail.com", "some-container", true);
    verify(storage, times(1)).makeSnapshot("some-container", "some-file-name.csv");
  }
}
