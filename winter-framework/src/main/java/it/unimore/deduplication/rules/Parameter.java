package it.unimore.deduplication.rules;

import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.comparators.RecordComparator;

public class Parameter {
		RecordComparator comparator;
		int priority;
		//Priority 2: if true, the rule is true. If false, continue
      	//Priority 1: if true, continue. If false end
      	//Priority 0: if true, the rule can be true. If false the rule can be false (check other parameters first)
		double weight;
		
		public Parameter(RecordComparator comparator, int priority, double weight) {
			super();
			this.comparator = comparator;
			this.priority = priority;
			this.weight = weight;
		}

		public RecordComparator getComparator() {
			return comparator;
		}

		public int getPriority() {
			return priority;
		}

		public double getWeight() {
			return weight;
		}
		
}
