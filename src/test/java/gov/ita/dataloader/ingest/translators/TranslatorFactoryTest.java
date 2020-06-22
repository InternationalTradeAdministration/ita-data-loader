package gov.ita.dataloader.ingest.translators;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

public class TranslatorFactoryTest {

  @Test
  public void returnsOtexaAnnualTranslator() {
    TranslatorFactory translatorFactory = new TranslatorFactory();
    Translator translator = translatorFactory.getTranslator("otexa#ANNUAL.csv");
    assertThat(translator, instanceOf(OtexaAnnualCsvTranslator.class));
    assertEquals(25000, translator.pageSize());
    assertEquals(TranslatorType.CSV, translator.type());
  }

  @Test
  public void returnsOtexaAnnualFootwearValueTranslator() {
    TranslatorFactory translatorFactory = new TranslatorFactory();
    Translator translator = translatorFactory.getTranslator("otexa#ANNUAL_FOOTWEAR_VALUE.csv");
    assertThat(translator, instanceOf(OtexaAnnualFootwearCsvTranslator.class));
    assertEquals(-1, translator.pageSize());
    assertEquals(TranslatorType.CSV, translator.type());
  }

  @Test
  public void returnsOtexaAnnualFootwearQtyTranslator() {
    TranslatorFactory translatorFactory = new TranslatorFactory();
    Translator translator = translatorFactory.getTranslator("otexa#ANNUAL_FOOTWEAR_QTY.csv");
    assertThat(translator, instanceOf(OtexaAnnualFootwearCsvTranslator.class));
    assertEquals(-1, translator.pageSize());
    assertEquals(TranslatorType.CSV, translator.type());
  }

  @Test
  public void returnsWorldBankEaseIndexTranslator() {
    TranslatorFactory translatorFactory = new TranslatorFactory();
    Translator translator = translatorFactory.getTranslator("select-usa#WORLDBANK_EASE_COUNTRY_INDEX.csv");
    assertThat(translator, instanceOf(WorldBankEaseIndexCsvTranslator.class));
    assertEquals(-1, translator.pageSize());
    assertEquals(TranslatorType.CSV, translator.type());
  }

  @Test
  public void returnsSimaTranslator() {
    TranslatorFactory translatorFactory = new TranslatorFactory();
    Translator translator = translatorFactory.getTranslator("sima#census/Dec2019P.txt");
    assertThat(translator, instanceOf(SimaCensusCsvTranslator.class));
    assertEquals(-1, translator.pageSize());
    assertEquals(TranslatorType.CSV, translator.type());
  }

}
