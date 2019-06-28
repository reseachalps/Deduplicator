package it.unimore.deduplication.rules.results;

public class FailureStateString extends State{

	@Override
	void behavior() {
		System.out.println("r1 is not equivalent to r2");
	}

}
