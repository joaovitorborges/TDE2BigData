package basic;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class chaveQ7 implements WritableComparable<chaveQ7> {
    String ano; // passamos n como 1 sempre para ajudar o reduce no processamento
    String fluxo;// passamos o peso

    public chaveQ7(){
    }

    public chaveQ7(String ano, String fluxo) {
        this.ano = ano;
        this.fluxo = fluxo;
    }

    public String getAno() { return ano; }

    public String getFluxo() {
        return fluxo;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ano = in.readUTF();
        fluxo = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(ano));
        out.writeUTF(String.valueOf(fluxo));

    }

    @Override
    public int compareTo(chaveQ7 o) {
        String valor1 = this.getFluxo()+this.getAno();
        String valor2 = o.getFluxo()+o.getAno();
        return (valor1.compareTo(valor2));
    }
}
