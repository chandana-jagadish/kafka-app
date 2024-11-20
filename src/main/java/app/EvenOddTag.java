package app;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

public class EvenOddTag {
    public static final TupleTag<KV<String, String>> EVEN_TAG = new TupleTag() {};
    public static final TupleTag<KV<String, String>> ODD_TAG = new TupleTag() {};
}
