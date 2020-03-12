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
        j.setReducerClass(Reducer4.class);

        //definicao dos tipos
        j.setOutputKeyClass(Text.class);
        j.setMapOutputValueClass(AuxQ4.class);

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

    public static class Mapper4 extends Mapper<LongWritable, Text, Text, AuxQ4> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();
            String[] colunas = linha.split(";");      //divide cada linha em palavras


            AuxQ4 outputValue = new AuxQ4(colunas[2],Float.parseFloat(colunas[6]));   //cria valor

            con.write((new Text(colunas[1])), outputValue); // manda a chave e o valor


        }
    }

    public static class Reducer4 extends Reducer<Text, AuxQ4, Text, IntWritable> {


        //1 parametro tipo da chave de entrada (saida do map)
        //2 parametro : tipo de valor de entrada (saida do map)
        //tipo de chave de saida
        //tipo de valor de saida

        // Funcao de reduce


        public void reduce(Text word, Iterable<AuxQ4> values, Context con)
                throws IOException, InterruptedException {

            float sum = 0;    // soma os valores

            for (AuxQ4 w:values) {

                sum += w.getPeso();

            }

            con.write(word,new IntWritable(sum)); // resultado final
        }
    }

}
