package com.revature.spark;

//simply prints the output

public class FilterResult {
	private String explanation;
	
	public FilterResult(String explanation) {
		super();
		this.explanation = explanation;
	}

	public String getExplanation() {
		return explanation;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((explanation == null) ? 0 : explanation.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		FilterResult other = (FilterResult) obj;
		if (explanation == null) {
			if (other.explanation != null)
				return false;
		} else if (!explanation.equals(other.explanation))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return explanation;
	}
}
