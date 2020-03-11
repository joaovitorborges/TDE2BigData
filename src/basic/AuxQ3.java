package basic;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AuxQ3 implements Writable {
    String mercadoria;
    int qnt;

    public AuxQ3(){
    }

    public AuxQ3(String mercadoria,int qnt) {
        this.mercadoria = mercadoria;
        this.qnt = qnt;
    }

    public String getMercadoria() {
        return mercadoria;
    }

    public int getQnt() {
        return qnt;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        mercadoria = in.readUTF();
        qnt = Integer.parseInt(in.readUTF());

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(mercadoria));
        out.writeUTF(String.valueOf(qnt));

    }
}
