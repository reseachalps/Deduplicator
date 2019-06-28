package it.unimore.comparators;


import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.comparators.StringComparator;
import de.uni_mannheim.informatik.dws.winter.similarity.string.LevenshteinSimilarity;
import it.unimore.cleaning.UrlCleaner;
import it.unimore.deduplication.model.Organization;

public class RecordComparatorOrganizationOpenAire extends StringComparator {

    boolean ignoreNull = false;

    /**
     * @param attributeRecord1
     * @param attributeRecord2
     */
    public RecordComparatorOrganizationOpenAire(Attribute attributeRecord1, Attribute attributeRecord2) {
        super(attributeRecord1, attributeRecord2);
    }

    public void setIgnoreNull(boolean ignoreNull) {
        this.ignoreNull = ignoreNull;
    }


    @Override
    public double compare(Record record1, Record record2, Correspondence<Attribute, Matchable> schemaCorrespondence) {


        String link1 = record1.getValue(Organization.LINK);
        if (link1 != null) {
            link1 = link1.trim().toLowerCase();
            link1 = cleanLink(link1);
        }
        String link2 = record2.getValue(Organization.LINK);
        if (link2 != null) {
            link2 = link2.trim().toLowerCase();
            link2 = cleanLink(link2);
        }


        String label1 = record1.getValue(Organization.LABEL);
        if (label1 != null) {
            label1 = label1.trim().toLowerCase();
        }
        String label2 = record2.getValue(Organization.LABEL);
        if (label2 != null) {
            label2 = label2.trim().toLowerCase();
        }


        String city1 = record1.getValue(Organization.CITY);
        if (city1 != null) {
            city1 = city1.trim().toLowerCase();
        }
        String city2 = record2.getValue(Organization.CITY);
        if (city2 != null) {
            city2 = city2.trim().toLowerCase();
        }


        String countryCode1 = record1.getValue(Organization.COUNTRY_CODE);
        if (countryCode1 != null) {
            countryCode1 = countryCode1.trim().toLowerCase();
        } else {
            System.err.println("NULL country code");
            return 0.0d;
        }
        String countryCode2 = record2.getValue(Organization.COUNTRY_CODE);
        if (countryCode2 != null) {
            countryCode2 = countryCode2.trim().toLowerCase();
        } else {
            System.err.println("NULL country code");
            return 0.0d;
        }


        if (!countryCode1.equals(countryCode2)) {
            return 0.0;
        } else {

            if (link1 != null && link2 != null) {
                if (link1.equals(link2)) {
//                    System.out.println("Equal link: ");
                    return 1.0d;
                } else {

                    return calculateSimilarity(label1, label2, city1, city2);
                }
            } else {

                return calculateSimilarity(label1, label2, city1, city2);
            }

        }


//        return 0;
    }


    private double calculateSimilarity(String label1, String label2, String city1, String city2) {
        LevenshteinSimilarity levenshteinSimilarity = new LevenshteinSimilarity();


        double labelSimilarity = 0.9d;
        if (label1 != null && label2 != null) {
            labelSimilarity = levenshteinSimilarity.calculate(label1, label2);
//            System.out.println("LABEL SIM: "+labelSimilarity);
        }


        double citySimilarity = 0.9d;
        if (city1 != null && city2 != null) {
            citySimilarity = levenshteinSimilarity.calculate(city1, city2);
//            System.out.println("CITY SIM: "+citySimilarity);
        }

        return (0.75 * labelSimilarity) + (0.25 * citySimilarity);
    }


    private String cleanLink(String link) {

        UrlCleaner cleaner = new UrlCleaner();
        return cleaner.clean(link);
    }
}
