package gov.ita.dataloader.ingest.translators;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.CharSequenceReader;

import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OtexaMergedCatImportsCsvTranslator implements Translator {

  private ScientificNotationTranslator snt = new ScientificNotationTranslator();

  @Override
  public byte[] translate(byte[] bytes) {
    StringWriter stringWriter = new StringWriter();
    CSVPrinter csvPrinter;

    try {
      csvPrinter = new CSVPrinter(stringWriter, CSVFormat.DEFAULT
        .withHeader("Country", "CAT_ID", "MERG_CAT", "HTS", "Description", "SYEF", "UOM", "M2", "DOLLAR_SIGN", "YR", "MON","HEADER_ID", "VAL"));

      Reader reader = new CharSequenceReader(new String(bytes));
      CSVParser csvParser;
      csvParser = new CSVParser(
        reader,
        CSVFormat.DEFAULT.withFirstRecordAsHeader().withTrim().withNullString("").withIgnoreHeaderCase());

      Map<String, Integer> headers = csvParser.getHeaderMap();

      List<String> valueFields = headers.keySet().stream()
        .filter(header -> (header.startsWith("D") && !header.startsWith("DESC")) || header.startsWith("QTY") || header.startsWith("VAL"))
        .collect(Collectors.toList());

      for (CSVRecord csvRecord : csvParser.getRecords()) {
        String country = csvRecord.get("country");
        String catId = csvRecord.get("CAT");
        String mergeCat = csvRecord.get("MERG CAT");
        String hts = csvRecord.get("HTS");
        String description = csvRecord.get("DESCRIP");
        String syef = csvRecord.get("SYEF");
        String uom = csvRecord.get("UOM");
        String m2 = csvRecord.get("M2");
        String dollarSign = csvRecord.get("$");
        String yr = csvRecord.get("YR");
        String mon = csvRecord.get("MON");

        for (String header : valueFields) {
          String val = csvRecord.get(header);
          if (val != null) {
            if (snt.isScientificNotation(val)) val = snt.translate(val);
            csvPrinter.printRecord(
              country, catId, mergeCat, hts, description, syef, uom, m2, dollarSign, yr, mon, header, val
            );
          }
        }
      }

      reader.close();
      csvPrinter.flush();

      return csvPrinter.getOut().toString().getBytes();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return null;
  }

  @Override
  public int pageSize() {
    return 25000;
  }

  @Override
  public TranslatorType type() {
    return TranslatorType.CSV;
  }
}
