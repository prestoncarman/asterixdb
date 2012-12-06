package com.ogeidix.lexergenerator;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Set;

public class LexerGenerator {
    private LinkedHashMap<String,Token> tokens = new LinkedHashMap<String, Token>();
    
    public void addToken(String rule) throws Exception{
        Token newToken;
        if(rule.charAt(0)=='@'){
            newToken = new TokenAux(rule, tokens);
        } else {
            newToken = new Token(rule, tokens);
        }
        Token existingToken = tokens.get(newToken.getName());
        if(existingToken==null){
            tokens.put(newToken.getName(), newToken);
        }else{
            existingToken.merge(newToken);
        }
    }

    public void generateLexer(HashMap<String,String> config) throws Exception{
        LexerNode main = this.compile();
        config.put("TOKENS_CONSTANTS",   this.tokensConstants());
        config.put("TOKENS_IMAGES",      this.tokensImages());
        config.put("LEXER_LOGIC",        main.toJava());
        config.put("LEXER_AUXFUNCTIONS", replaceParams(this.auxiliaryFunctions(main),config));
        String[] files   = {"Lexer.java", "LexerException.java"};
        String inputDir  = config.get("INPUT_DIR");
        String outputDir = config.get("OUTPUT_DIR");
        System.out.println("Input dir:\t" + inputDir);
        System.out.println("Output dir:\t" + outputDir);
        for(String file : files){
            System.out.print("Generating: " + file);
            String input  = readFile(inputDir + file);
            String output = replaceParams(input, config);
            file = file.replace("Lexer", config.get("LEXER_NAME"));
            System.out.print("\t>>\t" + file);
            FileWriter out = new FileWriter(outputDir+file);
            out.write(output);
            out.close();
            System.out.print(" [done]\n");
        }
    }

    public String printParsedGrammar() {
        StringBuilder result = new StringBuilder();
        for(Token token : tokens.values()){
            result.append(token.toString()).append("\n");
        }
        return result.toString();
    }

    private LexerNode compile() throws Exception {
        LexerNode main = new LexerNode();
        for(Token token : tokens.values()){
            if(token instanceof TokenAux) continue;
            main.merge(token.getNode());
        }
        return main;
    }

    private String tokensImages() {
        StringBuilder   result   = new StringBuilder();
        Set<String> uniqueTokens = tokens.keySet();
        for(String token : uniqueTokens){
            result.append(", \"<").append(token).append(">\" ");
        }
        return result.toString();
    }

    private String tokensConstants() {
        StringBuilder result       = new StringBuilder();
        Set<String>   uniqueTokens = tokens.keySet();
        int i=2;
        for(String token : uniqueTokens){
            result.append(", TOKEN_").append(token).append("=").append(i).append(" ");
            i++;
        }
        return result.toString();
    }

    private String auxiliaryFunctions(LexerNode main) {
        StringBuilder result    = new StringBuilder();
        Set<String>   functions = main.neededAuxFunctions();
        for(String token : functions){
            result.append("private int parse_"+token+"(char currentChar) throws IOException, [LEXER_NAME]Exception{\n");
            result.append(tokens.get(token).getNode().toJavaAuxFunction());
            result.append("\n}\n\n");
        }
        return result.toString();
    }

    private static String readFile(String fileName) throws FileNotFoundException, IOException {
        StringBuffer fileData = new StringBuffer(1000);
        BufferedReader reader = new BufferedReader(new FileReader(fileName));
        char[] buf = new char[1024];
        int numRead=0;
        while((numRead=reader.read(buf)) != -1){
            String readData = String.valueOf(buf, 0, numRead);
            fileData.append(readData);
            buf = new char[1024];
        }
        reader.close();
        return fileData.toString();
    }

    private static String replaceParams(String input, HashMap<String, String> config) {
        for(Entry<String, String> param : config.entrySet()){
            String key   = "\\[" + param.getKey() + "\\]";
            String value = param.getValue();
            input = input.replaceAll(key, value);
        }
        return input;
    }
 
    
    public static void main(String args[]) throws Exception{                
        if (args.length == 0 || args[0] == "--help" || args[0] == "-h"){
            System.out.println("LexerGenerator\nusage: java LexerGenerator <configuration file>");
            return;
        }
        
        LexerGenerator lexer = new LexerGenerator();
        HashMap<String, String> config = new HashMap<String, String>(); 

        System.out.println("Config file:\t"+args[0]);
        String input = readFile(args[0]);
        boolean tokens = false;
        for(String line : input.split("\r?\n")){
            line = line.trim();
            if (line.length() == 0 || line.charAt(0)=='#') continue;
            if(tokens == false && !line.equals("TOKENS:")){
                config.put(line.split("\\s*:\\s*")[0], line.split("\\s*:\\s*")[1]);
            } else if(line.equals("TOKENS:")) {
                tokens = true;
            } else {
                lexer.addToken(line);
            }
        }

        String parsedGrammar = lexer.printParsedGrammar();
        lexer.generateLexer(config);
        System.out.println("\nGenerated grammar:");
        System.out.println(parsedGrammar);
    }
    
}
