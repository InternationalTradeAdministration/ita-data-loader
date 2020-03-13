package gov.ita.dataloader.cron_jobs;

import com.fasterxml.jackson.databind.ObjectMapper;
import gov.ita.dataloader.TestHelpers;
import gov.ita.dataloader.business_unit.BusinessUnitService;
import gov.ita.dataloader.cron_jobs.StorageLogExtractor;
import gov.ita.dataloader.storage.BlobMetaData;
import gov.ita.dataloader.storage.Storage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)

public class StorageLogExtractorTest {

  @Mock
  private Storage storage;
  @Mock
  private BusinessUnitService businessUnitService;
  @Mock
  private ObjectMapper objectMapper;

  TestHelpers h = new TestHelpers();

  static byte[] COMIC_BYTES = "COMIC BYTES".getBytes();
  static byte[] MATRIX_BYTES = "MATRIX BYTES".getBytes();

  @Test
  public void extracts_blob_storage_metadata() throws Exception {
    when(businessUnitService.getStorageContainers()).thenReturn(Arrays.asList("comic-container", "matrix-container"));

    List<BlobMetaData> comicPii = new ArrayList<>();
    comicPii.add(h.buildBlobMetaData("batman.txt", "comic-container", "snap-1", 2024, 11, false));

    List<BlobMetaData> matrixNonPii = new ArrayList<>();
    matrixNonPii.add(h.buildBlobMetaData("neo.txt", "matrix-container", "snap-1", 2020, 1, true));

    when(storage.getBlobMetadata("comic-container", true)).thenReturn(comicPii);
    when(objectMapper.writeValueAsBytes(comicPii)).thenReturn(COMIC_BYTES);

    when(storage.getBlobMetadata("matrix-container", true)).thenReturn(matrixNonPii);
    when(objectMapper.writeValueAsBytes(matrixNonPii)).thenReturn(MATRIX_BYTES);

    StorageLogExtractor storageLogExtractor = new StorageLogExtractor(storage, businessUnitService, objectMapper);
    storageLogExtractor.extractStorageLog();

    verify(storage, times(1))
      .save("storage-log/comic-container.json", COMIC_BYTES, null, "dataloader", false, false);
    verify(storage, times(1))
      .save("storage-log/matrix-container.json", MATRIX_BYTES, null, "dataloader", false, false);
  }

}
