package basic;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class valorQ4 implements Writable {
    int n; // passamos n como 1 sempre para ajudar o reduce no processamento
    float peso;// passamos o peso

    public valorQ4(){
    }

    public valorQ4(int n, float peso) {
        this.n = n;
        this.peso = peso;
    }

    public int getN() { return n; }

    public float getPeso() {
        return peso;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        n = Integer.parseInt(in.readUTF());
        peso = Float.parseFloat(in.readUTF());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(n));
        out.writeUTF(String.valueOf(peso));

    }
}
