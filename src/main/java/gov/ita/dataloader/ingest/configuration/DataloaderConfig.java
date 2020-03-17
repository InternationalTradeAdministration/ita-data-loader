package gov.ita.dataloader.ingest.configuration;

import gov.ita.dataloader.business_unit.BusinessUnit;
import lombok.Data;

import java.util.List;

@Data
public class DataloaderConfig {
  public List<DataSetConfig> dataSetConfigs;
  public List<BusinessUnit> businessUnits;
}
