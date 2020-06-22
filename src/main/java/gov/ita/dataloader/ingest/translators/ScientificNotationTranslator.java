package gov.ita.dataloader.ingest.translators;

public class ScientificNotationTranslator {
  public boolean isScientificNotation(String val) {
    return val.contains("E+");
  }

  String translate(String val) {
    return String.format("%f", Double.parseDouble(val));
  }
}
