package gov.ita.dataloader.business_unit;

import lombok.Data;

import java.util.List;

@Data
public class BusinessUnitConfigResponse {
  public List<BusinessUnit> businessUnits;
}
