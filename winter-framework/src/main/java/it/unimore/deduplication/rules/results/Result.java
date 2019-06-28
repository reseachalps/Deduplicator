package it.unimore.deduplication.rules.results;

public interface Result {

	 void setSuccesState();
	 void setFailureState();
	 State getResultState();
	 void compute();
	 
}
