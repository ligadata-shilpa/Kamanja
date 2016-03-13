import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

final public class Bean implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private transient int b = 6;
	private int a = 5;

	public int getA() {
		return a;
	}

	public void setA(int a) {
		this.a = a;
	}

	public int getB() {
		return b;
	}

	public void setB(int b) {
		this.b = b;
	}

//	private void readObject(ObjectInputStream objectInputStream)
//			throws IOException, ClassNotFoundException {
//		System.out.println(">>>>>>>>>>>>>>> in");
//	}
//
//	private void writeObject(ObjectOutputStream objectOutputStream)
//			throws IOException {
//		System.out.println(">>>>>>>>>>>>>>> out");
//	}
}
