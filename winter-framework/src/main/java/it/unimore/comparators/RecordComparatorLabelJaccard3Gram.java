/*
 * Copyright (c) 2017 Data and Web Science Group, University of Mannheim, Germany (http://dws.informatik.uni-mannheim.de/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package it.unimore.comparators;

import de.uni_mannheim.informatik.dws.winter.matching.rules.Comparator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.comparators.StringComparator;
import de.uni_mannheim.informatik.dws.winter.similarity.string.LevenshteinSimilarity;
import org.simmetrics.metrics.Jaccard;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * {@link Comparator} for {@link Record}s based on the
 * values, and their {@link LevenshteinSimilarity}
 * similarity.
 *
 * @author Alexander Brinkmann (albrinkm@mail.uni-mannheim.de)
 */
public class RecordComparatorLabelJaccard3Gram extends StringComparator {


    boolean ignoreNull = false;

    public RecordComparatorLabelJaccard3Gram(Attribute attributeRecord1, Attribute attributeRecord2) {
        super(attributeRecord1, attributeRecord2);
    }

    private static final long serialVersionUID = 1L;
    private LevenshteinSimilarity sim = new LevenshteinSimilarity();


    public void setIgnoreNull(boolean ignoreNull) {
        this.ignoreNull = ignoreNull;
    }


    @Override
    public double compare(Record record1, Record record2, Correspondence<Attribute, Matchable> schemaCorrespondence) {

        String s1 = record1.getValue(this.getAttributeRecord1());
        String s2 = record2.getValue(this.getAttributeRecord2());
        s1 = preprocess(s1);
        s2 = preprocess(s2);

        //Ignore
        if (ignoreNull) {
            if (s1 == null || s2 == null) {
                return 1.0;
            }
        } else {
            if (s1 == null || s2 == null) {
                return 0.0;
            }
        }


        if (s1.contains("universit") && s2.contains("universit")) {
            s1 = s1.replace("universit", "");
            s2 = s2.replace("universit", "");
        }

        Set<String> gramsS1 = new HashSet<>();
        gramsS1.addAll(retrieve3Grams(s1));
        Set<String> gramsS2 = new HashSet<>();
        gramsS2.addAll(retrieve3Grams(s2));

        Jaccard<String> j = new Jaccard<>();
//        j

        // calculate similarity
//        double similarity = sim.calculate(s1, s2);

        return j.compare(gramsS1, gramsS2);
    }


    private Set<String> retrieve3Grams(String label) {
        Set<String> qgrams = new HashSet<>();


        int Q = 3;


        for (int i = 0; i < label.length() - Q; i++) {
            String qgram = label.substring(i, i + Q);
            if (!qgram.contains(" ")) {
                qgrams.add(qgram);
            }
        }

        return qgrams;

    }


}
