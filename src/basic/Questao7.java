package basic;

import com.google.inject.internal.cglib.core.$MethodInfoTransformer;
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


public class Questao7 {

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
        j.setJarByClass(Questao7.class);
        j.setMapperClass(Mapper7.class);
        //j.setReducerClass(Combine7.class);
        j.setReducerClass(Reducer7.class);

        //definicao dos tipos
        j.setOutputKeyClass(chaveQ7.class);
        j.setMapOutputValueClass(IntWritable.class);


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

    public static class Mapper7 extends Mapper<LongWritable, Text, chaveQ7, IntWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();
            String[] colunas = linha.split(";");      //divide cada linha em palavras


                chaveQ7 outputKey = new chaveQ7(colunas[1], colunas[4]);   //cria valor

                con.write(outputKey, new IntWritable(1)); // manda a chave e o valor

        }
    }

    public static class Combine7 extends Reducer<chaveQ7, IntWritable, chaveQ7, IntWritable> {

        public void reduce(chaveQ7 word, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int n = 0;

            for (IntWritable i:values) {
                n += i.get();
            }

            con.write(word,new IntWritable(n)); // resultado final
        }
    }


    public static class Reducer7 extends Reducer<chaveQ7, IntWritable, Text, Text> {


        public void reduce(chaveQ7 word, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int n = 0;

            for (IntWritable i:values) {
                n += i.get();
            }

            con.write(new Text(word.getAno()+" " + word.getFluxo() ),new Text(String.valueOf(n))); // resultado final
        }
    }

}
