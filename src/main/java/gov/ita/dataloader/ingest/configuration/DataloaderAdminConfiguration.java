package gov.ita.dataloader.ingest.configuration;

import lombok.Data;

import java.util.List;

@Data
public class DataloaderAdminConfiguration {
  public List<BusinessUnit> businessUnits;
}
