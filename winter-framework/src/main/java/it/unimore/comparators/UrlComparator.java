package it.unimore.comparators;

import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.comparators.StringComparator;
import de.uni_mannheim.informatik.dws.winter.similarity.EqualsSimilarity;

public class UrlComparator extends StringComparator {

    boolean ignoreNull = false;

    public UrlComparator(Attribute attributeRecord1, Attribute attributeRecord2) {
        super(attributeRecord1, attributeRecord2);
    }

    public void setIgnoreNull(boolean ignoreNull) {
        this.ignoreNull = ignoreNull;
    }

    private static final long serialVersionUID = 1L;
    private EqualsSimilarity<String> sim = new EqualsSimilarity<String>();


    @Override
    public double compare(Record record1, Record record2, Correspondence<Attribute, Matchable> schemaCorrespondence) {
        // preprocessing
        String s1 = record1.getValue(this.getAttributeRecord1());
        String s2 = record2.getValue(this.getAttributeRecord2());

        s1 = preprocess(s1);
        s2 = preprocess(s2);


        if (ignoreNull) {
            if (s1 == null || s2 == null) {
                return 1.0d;
            }
        }


        double similarity = sim.calculate(s1, s2);

        return similarity;
    }
}
