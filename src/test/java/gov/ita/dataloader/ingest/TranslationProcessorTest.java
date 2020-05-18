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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TranslationProcessorTest {

  @Mock
  Storage storage;

  @Mock
  TranslatorFactory translatorFactory;

  @Mock
  Translator translator;

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

    TranslationProcessor translationProcessor = new TranslationProcessor(storage, translatorFactory);
    translationProcessor.process("some-container", "some-file-name.csv", wholeFile);

    verify(storage).delete("some-container", "translated/some-file-name.csv");
    verify(storage).save(anyString(), eq(FIRST_TRANSLATED_BYTES), eq("system"), eq("some-container"), eq(true), eq(false));
    verify(storage).save(anyString(), eq(SECOND_TRANSLATED_BYTES), eq("system"), eq("some-container"), eq(true), eq(false));
    verify(storage).save(anyString(), eq(THIRD_TRANSLATED_BYTES), eq("system"), eq("some-container"), eq(true), eq(false));

  }

}
