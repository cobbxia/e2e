package test.transfer.generator;

public class FieldSchema implements Comparable {
	public String name = "", type = "";

	FieldSchema(String name, String type) {
		this.name = name;
		this.type = type;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof FieldSchema) {
			FieldSchema fs = (FieldSchema) obj;
			return (this.name == fs.name && this.type == fs.type);
		} else {
			return false;
		}
	}

	public String toString(String splitor) {
		return splitor + this.name + splitor + this.type.toUpperCase();
	}

	public String toString() {
		return this.toString("\t");
	}

	@Override
	public int compareTo(Object obj) {
		if (obj instanceof FieldSchema) {
			FieldSchema fs = (FieldSchema) obj;
			if (this.name.compareTo(fs.name) == 0)
				return this.type.compareTo(fs.type);
			else
				return this.name.compareTo(fs.name);
		}
		// TODO Auto-generated method stub
		return 0;
	}

}
