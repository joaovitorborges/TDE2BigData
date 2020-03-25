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


public class Questao6 {

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
        j.setJarByClass(Questao6.class);
        j.setMapperClass(Mapper6.class);
        j.setReducerClass(Reducer6.class);

        //definicao dos tipos
        j.setOutputKeyClass(Text.class);
        j.setMapOutputValueClass(valorQ6.class);


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

    public static class Mapper6 extends Mapper<LongWritable, Text, Text, valorQ6> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();
            String[] colunas = linha.split(";");      //divide cada linha em palavras
            valorQ6 outputValue;

            try {
                outputValue = new valorQ6(colunas[3], Float.parseFloat(colunas[5]));
            }
            catch(Exception E){
                outputValue = new valorQ6(colunas[3], 0);
            }
            con.write(new Text(colunas[7]),outputValue); // manda a chave e o valor

        }
    }


    public static class Reducer6 extends Reducer<Text, valorQ6, Text, Text> {


        //1 parametro tipo da chave de entrada (saida do map)
        //2 parametro : tipo de valor de entrada (saida do map)
        //tipo de chave de saida
        //tipo de valor de saida

        // Funcao de reduce


        public void reduce(Text word, Iterable<valorQ6> values, Context con)
                throws IOException, InterruptedException {

            float p = 0;
            String mercadoria = "";
            for (valorQ6 v:values) {
                if (v.getValor()>p){
                    p = v.getValor();
                    mercadoria =v.getMercadoria();
                }
            }


            con.write(word,new Text(mercadoria+"   "+ p)); // resultado final
        }
    }

}
