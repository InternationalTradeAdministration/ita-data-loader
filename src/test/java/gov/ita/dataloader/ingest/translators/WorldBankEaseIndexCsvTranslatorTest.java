package gov.ita.dataloader.ingest.translators;

import gov.ita.dataloader.TestHelpers;
import org.apache.commons.csv.CSVRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static gov.ita.dataloader.TestHelpers.formattedResults;
import static org.junit.Assert.assertEquals;

public class WorldBankEaseIndexCsvTranslatorTest {
  private TestHelpers h = new TestHelpers();

  private List<CSVRecord> results;

  @Before
  public void setUp() {
    WorldBankEaseIndexCsvTranslator worldBankEaseIndexCsvTranslator = new WorldBankEaseIndexCsvTranslator();
    byte[] translatedBytes = worldBankEaseIndexCsvTranslator.translate(h.get("/fixtures/select-usa/WORLDBANK_EASE_COUNTRY_INDEX.csv"));
    results = formattedResults(translatedBytes);
  }

  @Test
  public void translates_CountryName() {
    assertEquals("Afghanistan", results.get(0).get("Country_Name"));
  }

  @Test
  public void translates_CountryCode() {
    assertEquals("AFG", results.get(0).get("Country_Code"));
  }

  @Test
  public void translates_IndicatorName() {
    assertEquals("Ease of doing business index (1=most business-friendly regulations)", results.get(0).get("Indicator_Name"));
  }

  @Test
  public void translates_IndicatorCode() {
    assertEquals("IC.BUS.EASE.XQ", results.get(0).get("Indicator_Code"));
  }

  @Test
  public void translates_Year() {
    assertEquals("2019", results.get(0).get("Year"));
  }

  @Test
  public void translates_Val() {
    assertEquals("173", results.get(0).get("Val"));
  }

  @Test
  public void translates_Greece_2019() {
    List<CSVRecord> greece = results.stream().filter(r -> r.get("Country_Name").equals("Greece")).collect(Collectors.toList());
    assertEquals(2, greece.size());

    CSVRecord csvRecord = greece.stream().filter(r -> r.get("Year").equals("2019")).findFirst().get();
    assertEquals("79", csvRecord.get("Val"));
  }
}