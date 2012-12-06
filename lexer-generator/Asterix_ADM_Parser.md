The Asterix ADM Parser
======================

The ADM parser inside Asterix is composed by two different components:

* **The Parser** AdmTupleParser, which converts the adm tokens in internal objects
* **The Lexer**  AdmLexer, which scans the adm file and returns a list of adm tokens

These two classes belong to the package:

    edu.uci.ics.asterix.runtime.operators.file

The Parser is loaded through a factory (*AdmSchemafullRecordParserFactory*) by

    edu.uci.ics.asterix.external.dataset.adapter.FileSystemBasedAdapter extends AbstractDatasourceAdapter


How to add a new datatype
-------------------------
The ADM format allows two different kinds of datatype:

* primitive
* with constructor

A primitive datatype allows to write the actual value of the field without extra markup:

    { name : "Diego", age : 23 }

while the datatypes with constructor require to specify first the type of the value and then a string with the serialized value

    { center : point3d("P2.1,3,8.5") }

In order to add a new datatype the steps are:

1.  Add the new token to the **Lexer**
  * **if the datatype is primite** is necessary to create a TOKEN able to recognize **the format of the value**
  * **if the datatype is with constructor** is necessary to create **only** a TOKEN able to recognize **the name of the constructor**

2.  Change the **Parser** in order to convert correctly the new token in internal objects
  * This will require to **add new cases to the switch-case statements** and the introduction of **a serializer/deserializer object** for that datatype-.

The Lexer
----------
The lexer (the class AdmLexer) has to be manually generated and replaced after each change to the grammar.   
To regenerate the lexer is available the command line tool **lexer-generator** which need to be feed with the file **adm.grammar** that is present in the same directory of the class AdmLexer.
Inside the directory lexer-geneator is also available a README for that tool.

The steps to regenerate the lexer are:

1.  Change adm.grammar file
2.  Run lexer-generator

    java LexerGenerator configuration_file

3.  Replace AdmLexer with the new class



> Author: Diego Giorgini - diegogiorgini@gmail.com   
> 6 December 2012
