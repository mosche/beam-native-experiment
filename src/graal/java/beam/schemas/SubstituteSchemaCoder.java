package beam.schemas;

import com.oracle.svm.core.annotate.Alias;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.values.Row;

@TargetClass(SchemaCoder.class)
public final class SubstituteSchemaCoder {
  @Alias
  protected Schema schema;

  @Substitute
  private Coder<Row> getDelegateCoder(){
    throw new UnsupportedOperationException("SchemaCoder not supported [schema="+schema+"]");
  }
}
