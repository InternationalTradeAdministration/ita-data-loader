package gov.ita.dataloader;

import gov.ita.dataloader.storage.StorageInitializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.context.event.ContextRefreshedEvent;

import java.io.IOException;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DataloaderInitializerTest {

  @Mock
  private StorageInitializer storageInitializer;

  @Test
  public void onApplicationEvent() throws IOException {
    DataloaderInitializer dataloaderInitializer = new DataloaderInitializer(storageInitializer);
    dataloaderInitializer.onApplicationEvent(mock(ContextRefreshedEvent.class));
    verify(storageInitializer, times(1)).init();
  }
}