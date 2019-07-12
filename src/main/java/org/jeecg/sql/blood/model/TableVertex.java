package org.jeecg.sql.blood.model;

import java.io.Serializable;

public class TableVertex implements Serializable {

	private static final long serialVersionUID = -2761708446045917571L;

	private String lable;
	private String name;
	private String comment;

	public TableVertex(String lable, String name) {
		this.lable = lable;
		this.name = name;
	}

	public String getLable() {
		return lable;
	}

	public void setLable(String lable) {
		this.lable = lable;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
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
		result = prime * result + ((lable == null) ? 0 : lable.hashCode());
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
		TableVertex other = (TableVertex) obj;
		if (lable == null) {
			if (other.lable != null) {
                return false;
            }
		} else if (!lable.equals(other.lable)) {
            return false;
        }
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
		return lable+"\n"+name;
	}
}
