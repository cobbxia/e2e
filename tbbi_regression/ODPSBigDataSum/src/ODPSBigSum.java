import com.testyun.odps.udf.UDAF;
import com.testyun.odps.udf.UDAFEvaluator;

public class ODPSBigSum extends UDAF {
 public static class DoublePartialResult
 {
   public Long count;
   public Long sum;
 }
 public static class LongPartialResult
 {
   public Long count;
   public Long sum;
 }

public static class MyCountEvaluator implements UDAFEvaluator {
	private PartialResult partial;
   public MyCountEvaluator() {
     partial = new PartialResult();
   }
   public void init() {
     partial.count = new Long(0);
     partial.sum = new Long(0);
   }
   public void iterate(Long a) {
     if (a == null) {
       return;
     }
     partial.count += 1;
     partial.sum += a;
   }
   public PartialResult terminatePartial() {
     return partial;
   }
   public void merge(PartialResult pr) {
     if (pr == null) {
       return;
     }
     partial.count += pr.count;
     partial.sum += pr.sum;
   }
   public Double terminate() {
     if (partial.count == 0) {
       return 0.0;
     }
     return new Double(partial.sum / (double) partial.count.longValue());
   }
   public void setPartial(PartialResult pr) {
     partial = pr;
   }
 }


public static class DoubleSum implements UDAFEvaluator {
	private PartialResult partial;
   public DoubleSum(){
     partial = new PartialResult();
   }
   public void init() {
     partial.count = new Long(0);
     partial.sum = new Long(0);
   }
   public void iterate(String a) {
     if (a == null) {
       return;
     }
     partial.count += 1;
     partial.sum += a;
   }
   public PartialResult terminatePartial() {
     return partial;
   }
   public void merge(PartialResult pr) {
     if (pr == null) {
       return;
     }
     partial.count += pr.count;
     partial.sum += pr.sum;
   }
   public Double terminate() {
     if (partial.count == 0) {
       return 0.0;
     }
     return new Double(partial.sum / (double) partial.count.longValue());
   }
   public void setPartial(PartialResult pr) {
     partial = pr;
   }
 }


}