package it.unimore.deduplication.orcid;

import de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.RecordBlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import it.unimore.deduplication.model.Organization;

public class OrcidCountryCode extends RecordBlockingKeyGenerator<Record, Attribute> {
    @Override
    public void generateBlockingKeys(Record record, Processable<Correspondence<Attribute, Matchable>> correspondences, DataIterator<Pair<String, Record>> resultCollector) {


        if (record.hasValue(Organization.COUNTRY_CODE)) {

          //  String city = record.getValue(Organization.CITY);
            String cc = record.getValue(Organization.COUNTRY_CODE);
           /* city = city.replaceAll("\\(.*\\)","").trim();
            city =city.toLowerCase();*/
            cc = cc.replaceAll("\\(.*\\)","").trim();
            cc = cc.toLowerCase();

            resultCollector.next(new Pair<>((cc), record));




        }
    }
}
