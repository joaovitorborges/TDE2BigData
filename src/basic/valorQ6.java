package basic;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class valorQ6 implements Writable {
    String mercadoria; // passamos n como 1 sempre para ajudar o reduce no processamento
    float valor;// passamos o peso

    public valorQ6(){
    }

    public valorQ6(String mercadoria, float valor){
        this.mercadoria = mercadoria;
        this.valor = valor;
    }

    public String getMercadoria() { return mercadoria; }

    public float getValor() {
        return valor;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        mercadoria = String.valueOf(in.readUTF());
        valor = Float.parseFloat(in.readUTF());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(mercadoria));
        out.writeUTF(String.valueOf(valor));

    }
}
