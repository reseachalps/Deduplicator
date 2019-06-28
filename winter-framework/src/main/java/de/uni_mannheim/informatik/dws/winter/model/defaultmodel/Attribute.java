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

package de.uni_mannheim.informatik.dws.winter.model.defaultmodel;

import java.io.Serializable;
import java.util.Objects;

import de.uni_mannheim.informatik.dws.winter.model.Matchable;

/**
 * Default model of an attribute including an attribute name. 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class Attribute implements Matchable, Serializable {

	private static final long serialVersionUID = 1L;

	public Attribute() {
	}
	
	public Attribute(String identifier) {
		id = identifier;
	}
	
	public Attribute(String identifier, String provenance) {
		id = identifier;
		this.provenance = provenance;
	}
	
	private String name;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}

	protected String id;
	protected String provenance;

	@Override
	public String getIdentifier() {
		return id;
	}

	@Override
	public String getProvenance() {
		return provenance;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return getIdentifier();
	}


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Attribute attribute = (Attribute) o;
        return Objects.equals(id, attribute.id);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id);
    }
}
