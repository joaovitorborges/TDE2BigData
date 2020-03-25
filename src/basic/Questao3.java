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


public class Questao3 {

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
        j.setJarByClass(Questao3.class);
        j.setMapperClass(Mapper3.class);
        j.setReducerClass(Reducer3.class);

        //definicao dos tipos
        j.setOutputKeyClass(Text.class);
        j.setMapOutputValueClass(valorQ3.class);

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

    public static class Mapper3 extends Mapper<LongWritable, Text, Text, valorQ3> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();
            String[] colunas = linha.split(";");      //divide cada linha em palavras
            valorQ3 outputValue = null;
            if (colunas[0].equals("Brazil") && colunas[1].equals("2016") && colunas[4].equals("Import")) {
                try {
                     outputValue = new valorQ3(colunas[3], Integer.parseInt(colunas[8]));   //cria valor
                }
                catch(Exception E){
                     outputValue = new valorQ3(colunas[3], 0);   //cria valor

                }
                con.write((new Text("mercadoria")), outputValue);
            }

        }
    }

    public static class Reducer3 extends Reducer<Text, valorQ3, Text, IntWritable> {


        //1 parametro tipo da chave de entrada (saida do map)
        //2 parametro : tipo de valor de entrada (saida do map)
        //tipo de chave de saida
        //tipo de valor de saida

        // Funcao de reduce


        public void reduce(Text word, Iterable<valorQ3> values, Context con)
                throws IOException, InterruptedException {

            int val = 0;    // soma os valores
            String commodity = "";
            for (valorQ3 w:values) {
                if (w.getQnt() > val) {
                    commodity = w.getMercadoria();
                    val = w.getQnt();
                }
            }

            con.write(new Text(commodity),new IntWritable(val)); // resultado final
        }
    }

}
