package it.unimore.comparators;


import com.shekhargulati.urlcleaner.UrlCleaner;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.comparators.StringComparator;
import de.uni_mannheim.informatik.dws.winter.similarity.string.LevenshteinSimilarity;
import it.unimore.deduplication.model.Organization;
import org.apache.jena.base.Sys;


import javax.lang.model.type.ArrayType;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RecordComparatorOrganization extends StringComparator {

    boolean ignoreNull = false;
    boolean debug = false;


    static List<String> baseUrlToExlude = new ArrayList<>();

    /**
     * @param attributeRecord1
     * @param attributeRecord2
     */
    public RecordComparatorOrganization(Attribute attributeRecord1, Attribute attributeRecord2) {
        super(attributeRecord1, attributeRecord2);
    }

    public void setIgnoreNull(boolean ignoreNull) {
        this.ignoreNull = ignoreNull;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }


    public void setbaseUrlToExlude(List<String> baseUrlToExlude) {
        this.baseUrlToExlude = baseUrlToExlude;
    }

    @Override
    public double compare(Record record1, Record record2, Correspondence<Attribute, Matchable> schemaCorrespondence) {


        Set<String> links1 = new HashSet<>();
        Set<String> links2 = new HashSet<>();


        String link1 = record1.getValue(Organization.LINK);
        if (link1 != null) {
            link1 = link1.trim().toLowerCase();
            // link1 = cleanLink(link1);
            links1.addAll(parseLink(link1, debug));
        }
        String link2 = record2.getValue(Organization.LINK);
        if (link2 != null) {
            link2 = link2.trim().toLowerCase();
            // link2 = cleanLink(link2);
            links2.addAll(parseLink(link2, debug));
        }

        String id1 = record1.getValue(Organization.ID);
        String id2 = record2.getValue(Organization.ID);

        if (id1.equals(id2)) {
            return 0.0d;
        }


        String label1 = record1.getValue(Organization.LABEL);
        if (label1 != null) {
            label1 = label1.trim().toLowerCase();
        }
        String label2 = record2.getValue(Organization.LABEL);
        if (label2 != null) {
            label2 = label2.trim().toLowerCase();
        }


        String city1 = record1.getValue(Organization.NUTS_3);
        if (city1 != null) {
            city1 = city1.trim().toLowerCase();
        }
        String city2 = record2.getValue(Organization.NUTS_3);
        if (city2 != null) {
            city2 = city2.trim().toLowerCase();
        }


        String address1 = record1.getValue(Organization.ADDRESS);
        if (address1 != null) {
            address1 = address1.trim().toLowerCase();
        }
        String address2 = record2.getValue(Organization.ADDRESS);
        if (address2 != null) {
            address2 = address2.trim().toLowerCase();
        }

        String countryCode1 = record1.getValue(Organization.COUNTRY_CODE);
        if (countryCode1 != null) {
            countryCode1 = countryCode1.trim().toLowerCase();
        } else {
            System.err.println("NULL country code");
            if (debug) {
                System.out.println("MATCH: ");
                System.out.println(record1.getValue(Organization.ID) + "\t" + record1.getValue(Organization.LABEL) + "\t" + record1.getValue(Organization.ADDRESS) + "\t" + record1.getValue(Organization.CITY) + "\t" + record1.getValue(Organization.LINK));
                System.out.println(record2.getValue(Organization.ID) + "\t" + record2.getValue(Organization.LABEL) + "\t" + record2.getValue(Organization.ADDRESS) + "\t" + record2.getValue(Organization.CITY) + "\t" + record2.getValue(Organization.LINK));

                System.out.println("SIM: " + 0);
            }
            return 0.0d;
        }
        String countryCode2 = record2.getValue(Organization.COUNTRY_CODE);
        if (countryCode2 != null) {
            countryCode2 = countryCode2.trim().toLowerCase();
        } else {
            System.err.println("NULL country code");
            if (debug) {
                System.out.println("MATCH: ");
                System.out.println(record1.getValue(Organization.ID) + "\t" + record1.getValue(Organization.LABEL) + "\t" + record1.getValue(Organization.ADDRESS) + "\t" + record1.getValue(Organization.CITY) + "\t" + record1.getValue(Organization.LINK));
                System.out.println(record2.getValue(Organization.ID) + "\t" + record2.getValue(Organization.LABEL) + "\t" + record2.getValue(Organization.ADDRESS) + "\t" + record2.getValue(Organization.CITY) + "\t" + record2.getValue(Organization.LINK));

                System.out.println("SIM: " + 0);
            }
            return 0.0d;
        }


        List<String> staff1 = new ArrayList<>();
        List<String> staff2 = new ArrayList<>();


        String staff1String = record1.getValue(Organization.STAFF);
        String staff2String = record2.getValue(Organization.STAFF);

        if (staff1String != null) {
            staff1.addAll(parseStaff(staff1String));
        }

        if (staff2String != null) {
            staff2.addAll(parseStaff(staff2String));
        }


        if (!countryCode1.equals(countryCode2)) {
            if (debug) {
                System.out.println("SIM: " + 0);
            }
            return 0.0;
        } else {

            if (link1 != null && link2 != null) {

                boolean comparisonBetweenLink = false;

                for (String l1 : links1) {
                    for (String l2 : links2) {
                        comparisonBetweenLink = true;
                        if (l1.equals(l2)) {
                            if (debug) {
                                System.out.println("EQUAL LINK!!!");
                                System.out.println("SIM: " + 1);
                            }
                            return 1.0d;
                        }
                    }
                }

//                if (comparisonBetweenLink) {
//                    if (debug) {
//                        System.out.println("Different LINKS");
//                    }
//                    return 0.0d;
//                }

                if (link1.equals(link2)) {
                    if (debug) {
                        System.out.println("EQUAL LINK");
                        System.out.println("SIM: " + 1);
                    }
                    return 1.0d;
                } else {

                    double sim = calculateSimilarity(label1, label2, city1, city2, address1, address2, staff1, staff2, debug);
                    if (debug) {
                        System.out.println("CASE2");
                        System.out.println("MATCH: ");
                        System.out.println(record1.getValue(Organization.ID) + "\t" + record1.getValue(Organization.LABEL) + "\t" + record1.getValue(Organization.ADDRESS) + "\t" + record1.getValue(Organization.CITY) + "\t" + record1.getValue(Organization.LINK));
                        System.out.println(record2.getValue(Organization.ID) + "\t" + record2.getValue(Organization.LABEL) + "\t" + record2.getValue(Organization.ADDRESS) + "\t" + record2.getValue(Organization.CITY) + "\t" + record2.getValue(Organization.LINK));

                        System.out.println("SIM: " + sim);
                    }
                    return sim;

                }
            } else {


                double sim = calculateSimilarity(label1, label2, city1, city2, address1, address2, staff1, staff2, debug);
                if (debug) {
                    System.out.println("CASE3");
                    System.out.println("MATCH: ");
                    System.out.println(record1.getValue(Organization.ID) + "\t" + record1.getValue(Organization.LABEL) + "\t" + record1.getValue(Organization.ADDRESS) + "\t" + record1.getValue(Organization.CITY) + "\t" + record1.getValue(Organization.LINK));
                    System.out.println(record2.getValue(Organization.ID) + "\t" + record2.getValue(Organization.LABEL) + "\t" + record2.getValue(Organization.ADDRESS) + "\t" + record2.getValue(Organization.CITY) + "\t" + record2.getValue(Organization.LINK));

                    System.out.println("SIM: " + sim);
                }
                return sim;

            }

        }


//        return 0;
    }

    public static List<String> parseStaff(String staffString) {
        List<String> staff = new ArrayList<>();

        if (staffString == null) {
            return staff;
        }


        if (!staffString.trim().equals("")) {
            String l = staffString.trim();

            if (l.contains(";")) {
                String[] people = l.split(";");
                for (String person : people) {
                    staff.add(person);
                }
            } else {
                staff.add(l);
            }


        }

        return staff;


    }


    public static List<String> parseLink(String staffString, boolean print) {


        List<String> staff = new ArrayList<>();

        if (staffString == null) {
            return staff;
        }


        if (!staffString.trim().equals("")) {
            String l = staffString.trim();

            if (l.contains(";")) {
                String[] people = l.split(";");
                for (String person : people) {
                    staff.add(getUrlBase(person));
                }
            } else {
                staff.add(getUrlBase(l));
            }
        }


        if (print) {
            System.out.println("Original string: " + staffString);
            System.out.println("======LINKS======");
            for (String link : staff) {
                System.out.println("\t" + link);
            }
            System.out.println("======LINKS======");
        }


        return staff;


    }


    private static String getUrlBase(String link) {


        String initialLink = link;


        URL url = null;

        try {
            link = link.trim();
            link = UrlCleaner.normalizeUrl(link);
            url = new URL(link);
            url.getHost();


            if (baseUrlToExlude.contains(url.getHost())) {
                url = null;
            }
        } catch (MalformedURLException e) {
            // e.printStackTrace();
            // System.out.println("Malformed url: " + initialLink);
            //System.exit(0);
        } catch (Exception e) {
            // e.printStackTrace();
            // System.out.println("Malformed url 1: " + initialLink);
            //System.exit(0);
        }

        //System.out.println(url.getProtocol() + "://" + url.getHost());


        //UrlCleaner cleaner = new UrlCleaner();

        if (url != null) {
            return url.getHost();
        } else {
            return initialLink;
        }
    }


    private double calculateSimilarity(String label1, String label2, String city1, String city2, String address1, String address2, List<String> staff1, List<String> staff2, boolean print) {
        LevenshteinSimilarity levenshteinSimilarity = new LevenshteinSimilarity();


        for (String person : staff1) {
            if (staff2.contains(person)) {
                if (city1 != null && city2 != null) {

                    if (!city1.equals(city2)) {
                        System.out.println("\t\tSAME PERSON BUT DIFFERENT CITIES!!");
                        continue;
                    }
                }

                System.out.println("\t\tSAME STAFF!!! SAME STRUCTURE");
                return 1.0d;
            }
        }


        double labelSimilarity = 1.0d;
        if (label1 != null && label2 != null) {
            labelSimilarity = levenshteinSimilarity.calculate(label1, label2);
//            System.out.println("LABEL SIM: "+labelSimilarity);
        }


        double citySimilarity = 0.9d;
        if (city1 != null && city2 != null) {
            if (!city1.equals(city2)) {
                return 0.0d;
            }

            citySimilarity = levenshteinSimilarity.calculate(city1, city2);
//            System.out.println("CITY SIM: "+citySimilarity);
        }

        double addressSimilarity = 0.9d;
        if (address1 != null && address2 != null) {
            addressSimilarity = levenshteinSimilarity.calculate(address1, address2);
//            System.out.println("CITY SIM: "+citySimilarity);
        }


        if (address1 != null && address2 != null) {

            if (debug) {
                System.out.println("\tADDRESS NOT NULL");

            }

            return (0.3 * labelSimilarity) + (0.5 * addressSimilarity) + (0.2 * citySimilarity);
        } else if (city1 != null && city2 != null) {
            if (debug) {
                System.out.println("\tADDRESS  IS  NULL");
                System.out.println("\tCITY NOT NULL");
            }


            return (0.5 * labelSimilarity) + (0.5 * citySimilarity);


        } else {
            if (debug) {
                System.out.println("CITY NOT NULL");
            }
            return (0.5 * labelSimilarity) + (0.25 * addressSimilarity) + (0.25 * citySimilarity);
        }

    }

    public List<String> getBaseUrlToExlude() {
        return baseUrlToExlude;
    }

    public void setBaseUrlToExlude(List<String> baseUrlToExlude) {
        this.baseUrlToExlude = baseUrlToExlude;
    }

/*
    private String cleanLink(String link) {

        URL url = null;
        try {
            url = new URL(link);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

        //System.out.println(url.getProtocol() + "://" + url.getHost());


        UrlCleaner cleaner = new UrlCleaner();
        //return cleaner.clean(url.getHost());
        return cleaner.clean(link);
    }

    */
}
