package com.ogeidix.lexergenerator;

import java.util.LinkedHashMap;

public class TokenAux extends Token {

    public TokenAux(String str, LinkedHashMap<String, Token> tokens) throws Exception {
        super(str, tokens);
    }

}
