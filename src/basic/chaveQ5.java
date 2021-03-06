package basic;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class chaveQ5 implements WritableComparable<chaveQ5> {
    String ano; // passamos n como 1 sempre para ajudar o reduce no processamento
    String mercadoria;// passamos o peso

    public chaveQ5(){
    }

    public chaveQ5(String ano, String mercadoria) {
        this.ano = ano;
        this.mercadoria = mercadoria;
    }

    public String getAno() { return ano; }

    public String getMercadoria() {
        return mercadoria;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ano = in.readUTF();
        mercadoria = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(ano));
        out.writeUTF(String.valueOf(mercadoria));

    }

    @Override
    public int compareTo(chaveQ5 o) {
        String valor1 = this.getMercadoria()+this.getAno();
        String valor2 = o.getMercadoria()+o.getAno();
        return (valor2.compareTo(valor1));
    }
}
