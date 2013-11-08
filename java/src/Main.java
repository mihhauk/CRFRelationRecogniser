import liner2.LinerOptions;
import liner2.chunker.Chunker;
import liner2.chunker.factory.ChunkerFactory;
import liner2.chunker.factory.ChunkerManager;
import liner2.features.TokenFeatureGenerator;
import liner2.reader.ReaderFactory;
import liner2.reader.StreamReader;
import liner2.structure.Annotation;
import liner2.structure.AnnotationSet;
import liner2.structure.ParagraphSet;
import liner2.structure.Sentence;

import java.util.HashMap;
import java.util.HashSet;

/**
 * Created with IntelliJ IDEA.
 * User: michal
 * Date: 11/5/13
 * Time: 4:41 PM
 * To change this template use File | Settings | File Templates.
 */
public class Main {

    public static void main(String[] args) throws Exception{

        StreamReader reader = ReaderFactory.get().getStreamReader("/home/michal/repo/projekt_inz/threads/annotated_context.xml", "ccl");
        ParagraphSet ps = reader.readParagraphSet();

        HashMap<Sentence, AnnotationSet> old_chunks = ps.getChunkings();


        LinerOptions opts = new LinerOptions();
        opts.loadIni("/home/michal/repo/CRFRelationRecogniser/configs/ini/config-load.ini");
        System.out.println("Loaded ini");
        TokenFeatureGenerator gen = new TokenFeatureGenerator(opts.features);
        gen.generateFeatures(ps);
        System.out.println("generated feats");

        ChunkerManager cm = ChunkerFactory.loadChunkers(opts);
        System.out.println("loaded chunkers");
        Chunker chunker = cm.getChunkerByName(opts.getOptionUse());
        System.out.println("got chunker");

        HashMap<Sentence, AnnotationSet> result = chunker.chunk(ps);
        System.out.println("Chunked");


        int diff = 0;
        int unrecognized = 0;
        int diffrentContext = 0;
        int falsepositive = 0;
        int morethatone = 0;
        for(Sentence s: result.keySet()){

            HashSet<String> newAnnsText = new HashSet<String>();
            for(Annotation ch: result.get(s).chunkSet()){
                newAnnsText.add(ch.getText());
            }
            HashSet<String> oldAnnsText = new HashSet<String>();
            for(Annotation ch: old_chunks.get(s).chunkSet()){
                oldAnnsText.add(ch.getText());
            }


            if(!newAnnsText.equals(oldAnnsText)){

                if(newAnnsText.size() == oldAnnsText.size()){
                    diffrentContext++;
                }
                else if( newAnnsText.size() > 1){
                    morethatone++;
                }
                else if( newAnnsText.isEmpty()){
                    unrecognized++;
                }
                else if( oldAnnsText.isEmpty()){
                    falsepositive++;
                }

                diff++;
                System.out.println("Sentence:");


                System.out.println("Anns old: "+oldAnnsText.size());
                for(String ann: oldAnnsText){
                    System.out.println(ann);
                }
                System.out.println("Anns new: "+newAnnsText.size());
                for(String ann: newAnnsText){
                    System.out.println(ann);
                }
                System.out.println("---------------------");
            }
        }
        System.out.println("Różniące się zdania: "+diff+"/"+result.size()+" | "+diff/(double)result.size());
        System.out.println("Ilość zdań z nierozpoznaną relacją: "+unrecognized);
        System.out.println("Ilość zdań z niepoprawnie oznaczonym kontekstem relacji: "+diffrentContext);
        System.out.println("Ilość zdań z niepoprawnie rozpoznaną relacją: "+falsepositive);
        System.out.println("Ilość zdań z rozpoznaną więcej niż 1 relacją: "+morethatone);

    }

}
