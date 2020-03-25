package basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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


public class Questao4 {

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
        j.setJarByClass(Questao4.class);
        j.setMapperClass(Mapper4.class);
        j.setReducerClass(Combine4.class);
        j.setReducerClass(Reducer4.class);

        //definicao dos tipos
        j.setOutputKeyClass(chaveQ4.class);
        j.setMapOutputValueClass(valorQ4.class);


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

    public static class Mapper4 extends Mapper<LongWritable, Text, chaveQ4, valorQ4> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();
            String[] colunas = linha.split(";");      //divide cada linha em palavras

            chaveQ4 outputKey = new chaveQ4(colunas[1], colunas[3]);   //cria valor

            valorQ4 outputValue;

            try {
                outputValue = new valorQ4(1,Long.parseLong(colunas[6]));
            }catch(Exception E){
                outputValue = new valorQ4(1,0);
            }



            con.write(outputKey,outputValue); // manda a chave e o valor

        }
    }


    public static class Combine4 extends Reducer<chaveQ4, valorQ4, chaveQ4, valorQ4> {


        public void reduce(chaveQ4 word, Iterable<valorQ4> values, Context con)
                throws IOException, InterruptedException {

            long peso = 0;    // soma os valores
            int n = 0;
            for (valorQ4 v:values) {
                peso += v.getPeso();
                n += v.getN();
            }


            valorQ4 outputValue2 = new valorQ4(n,peso);

            con.write(word,outputValue2); // resultado final
        }
    }


    public static class Reducer4 extends Reducer<chaveQ4, valorQ4, Text, Text> {


        //1 parametro tipo da chave de entrada (saida do map)
        //2 parametro : tipo de valor de entrada (saida do map)
        //tipo de chave de saida
        //tipo de valor de saida

        // Funcao de reduce


        public void reduce(chaveQ4 word, Iterable<valorQ4> values, Context con)
                throws IOException, InterruptedException {

            long peso = 0;    // soma os valores
            int n = 0;
            for (valorQ4 v:values) {
                peso += v.getPeso();
                n += v.getN();
            }

            long media  = peso/n;

            con.write(new Text(word.getAno()+ " " + word.getMercadoria()),new Text(String.valueOf(media))); // resultado final
        }
    }

}
