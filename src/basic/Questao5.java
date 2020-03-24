package basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;


public class Questao5 {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path(files[0]);
        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "wordcount-professor");

        //cadastro das classes
        j.setJarByClass(Questao5.class);
        j.setMapperClass(Mapper5.class);
        j.setReducerClass(Combine5.class);
        j.setReducerClass(Reducer5.class);

        //definicao dos tipos
        j.setOutputKeyClass(chaveQ5.class);
        j.setMapOutputValueClass(valorQ5.class);


        //definindo arquivos de entrada e saida
        FileInputFormat.addInputPath(j,input);
        FileOutputFormat.setOutputPath(j,output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);

    }


    //Classe MAP
    //1 parametro tipo da chave de entrada
    //2 parametro : tipo de valor de entrada
    //tipo de chave de saida
    //tipo de valor de saida

    public static class Mapper5 extends Mapper<LongWritable, Text, chaveQ5, valorQ5> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();
            String[] colunas = linha.split(";");      //divide cada linha em palavras

            if(colunas[0].equals("Brazil")){
                chaveQ5 outputKey = new chaveQ5(colunas[1], colunas[3]);   //cria valor
                valorQ5 outputValue = new valorQ5(1, Float.parseFloat(colunas[6]));

                con.write(outputKey, outputValue); // manda a chave e o valor
            }
        }
    }


    public static class Combine5 extends Reducer<chaveQ5, valorQ5, chaveQ5, valorQ5> {


        public void reduce(chaveQ5 word, Iterable<valorQ5> values, Context con)
                throws IOException, InterruptedException {

            float peso = 0;    // soma os valores
            float n = 0;
            for (valorQ5 v:values) {
                peso += v.getPeso();
                n += v.getN();
            }

            peso = peso/n;

            valorQ5 outputValue2 = new valorQ5((int) n,peso);

            con.write(word,outputValue2); // resultado final
        }
    }


    public static class Reducer5 extends Reducer<chaveQ5, valorQ5, Text, Text> {


        //1 parametro tipo da chave de entrada (saida do map)
        //2 parametro : tipo de valor de entrada (saida do map)
        //tipo de chave de saida
        //tipo de valor de saida

        // Funcao de reduce


        public void reduce(chaveQ5 word, Iterable<valorQ5> values, Context con)
                throws IOException, InterruptedException {

            float peso = 0;    // soma os valores
            float n = 0;
            for (valorQ5 v:values) {
                peso += v.getPeso();
                n += v.getN();
            }

            peso = peso/n;

            con.write(new Text(word.getAno()+ " " + word.getMercadoria()),new Text(String.valueOf(peso))); // resultado final
        }
    }

}
