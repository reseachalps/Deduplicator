package it.unimore.deduplication.rules.results;

public class ResultString implements Result{
		State succes = new SuccesStateString();
		State failure = new FailureStateString();
		State current;
		
		public ResultString() {
			super();
			current = failure;
		}

		@Override
		public void setSuccesState() {
			current = succes;
			
		}

		@Override
		public State getResultState() {
			return current;
			
		}

		@Override
		public void setFailureState() {
			current = failure;
			
		}

		@Override
		public void compute() {
			current.behavior();
		}
		
		
}	
