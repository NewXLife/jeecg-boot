package org.jeecg.sql.blood.model;

import java.io.Serializable;

public class Field implements Serializable{
	private static final long serialVersionUID = -3638022676006851746L;
	
	private String name;
	private String type;
	private String comment;

	public Field() {
	}

	public Field(String name, String type) {
		this.name = name;
		this.type = type;
	}

	public Field(String name, String type, String comment) {
		this.name = name;
		this.type = type;
		this.comment = comment;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
            return true;
        }
		if (obj == null) {
            return false;
        }
		if (getClass() != obj.getClass()) {
            return false;
        }
		Field other = (Field) obj;
		if (name == null) {
			if (other.name != null) {
                return false;
            }
		} else if (!name.equals(other.name)) {
            return false;
        }
		return true;
	}

	@Override
	public String toString() {
		return "Field [name=" + name + ", type=" + type + ", comment=" + comment + "]";
	}
}
