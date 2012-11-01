package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import java.io.IOException;
import java.io.PrintStream;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.data.utils.WriteValueTools;

public class AInt64Printer implements IPrinter {

    private static final long serialVersionUID = 1L;

    public static final AInt64Printer INSTANCE = new AInt64Printer();

    @Override
    public void init() {
    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        long d = AInt64SerializerDeserializer.getLong(b, s + 1);
        try {
            WriteValueTools.writeLong(d, ps);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }
}