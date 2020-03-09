package gov.ita.dataloader.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import gov.ita.dataloader.ingest.configuration.DataloaderConfig;
import gov.ita.dataloader.security.AuthenticationFacade;
import gov.ita.dataloader.storage.Storage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.Collections;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class IngestControllerTest {

  @Mock
  private Storage storage;

  @Mock
  private AutomatedIngestProcessor automatedIngestProcessor;

  @Mock
  private AuthenticationFacade authenticationFacade;

  @Mock
  private ObjectMapper objectMapper;

  @Mock
  private TranslationProcessor translationProcessor;

  private static byte[] DATALOADER_CONFIG = "dataloader blob".getBytes();
  private static byte[] SOME_BYTES = "some bytes".getBytes();

  @Before
  public void setUp() throws IOException {
    when(authenticationFacade.getUserName()).thenReturn("TestUser@gmail.com");
    when(storage.getBlob("some-container", "configuration.json")).thenReturn(DATALOADER_CONFIG);
    DataloaderConfig dataloaderConfig = new DataloaderConfig();
    dataloaderConfig.setDataSetConfigs(Collections.emptyList());
    when(objectMapper.readValue(DATALOADER_CONFIG, DataloaderConfig.class)).thenReturn(dataloaderConfig);
  }

  @Test
  public void startsIngestProcess() {
    IngestController ingestController = new IngestController(storage, automatedIngestProcessor, authenticationFacade, objectMapper, translationProcessor);
    ingestController.startIngestProcess("some-container");

    verify(automatedIngestProcessor, times(1))
      .process(Collections.emptyList(), "some-container", "TestUser@gmail.com", 5000L);
  }

  @Test
  public void uploadedFileIsSavedAndPassedToTranslationProcessor() throws IOException {
    IngestController ingestController = new IngestController(storage, automatedIngestProcessor, authenticationFacade, null, translationProcessor);
    MultipartFile multipartFile = mock(MultipartFile.class);
    when(multipartFile.getBytes()).thenReturn(SOME_BYTES);
    when(multipartFile.getOriginalFilename()).thenReturn("OG File Name.csv");

    ingestController.saveFile(multipartFile, "cool-container", false);

    verify(storage, times(1)).save("OG File Name.csv", SOME_BYTES, "TestUser@gmail.com", "cool-container", true, false);
    verify(storage, times(1)).makeSnapshot("cool-container", "OG File Name.csv");
    verify(translationProcessor, times(1))
      .initProcessing("cool-container", "OG File Name.csv", SOME_BYTES, "TestUser@gmail.com");
  }

  @Test
  public void uploadedFilePassedThroughTranslationProcessor() throws IOException {
    IngestController ingestController = new IngestController(storage, null, authenticationFacade, null, translationProcessor);
    MultipartFile multipartFile = mock(MultipartFile.class);
    when(multipartFile.getOriginalFilename()).thenReturn("OG File Name.csv");
    when(multipartFile.getBytes()).thenReturn(SOME_BYTES);

    ingestController.saveFile(multipartFile, "cool-container", false);

    verify(translationProcessor, times(1))
      .initProcessing("cool-container", "OG File Name.csv", SOME_BYTES, "TestUser@gmail.com");
  }

}
