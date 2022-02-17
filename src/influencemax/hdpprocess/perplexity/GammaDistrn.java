/*
 * GammaDistribution.java
 *
 * Created on 2015.3
 *
 * author: Zhuo
 */

package influencemax.hdpprocess.perplexity;


import java.io.IOException;
import java.util.*;
import cern.jet.random.Gamma;
//import oldcode.HDPTopicModel.DirichletProcess;
//import cern.jet.random.Beta;
//import cern.jet.random.Gamma;
//import cern.jet.random.Uniform;

/**
 *
 * @author Zhuo
 */
public class GammaDistrn implements ProbDistribution {
    
	public double m_gamma;
    /** Creates a new instance of GammaDistribution */
    public GammaDistrn(double shape, double scale){
        this.shape = shape;
        this.scale = scale;
//        System.out.println(Gamma.staticNextDouble(shape,scale));
        m_gamma = Gamma.staticNextDouble(shape,1/scale);
//        System.out.println(m_gamma);
//        norm = Math.log( scale ) * shape + GammaFunction.logGamma( shape );
//        if( shape < 1 )
//            b = (Math.E + shape)/Math.E;
//        else if( shape > 1){
//            a = Math.sqrt(2 * shape -1);
//            b = shape - Math.log(4.0);
//            q = shape + 1 /a;
//            d = 1+ Math.log(4.5);
//        }
//        
//        double r;
//        if(shape >1)
//            r = randomForShapeGreaterThan1();
//        else if(shape < 1)
//            r = randomForShapeLessThan1();
//        else
//            r = randomForShapeEqualTo1();
//        m_gamma = r * scale;
    }
    
    public void setShape(double shape){this.shape = shape;}
    public void setScale(double scale){this.scale = scale;}
    public double getShape(){return shape;}
    public double getScale(){return scale;}
    
    public double getValue(){
    	return m_gamma;
        
    }
    
    private double randomForShapeEqualTo1(){
        return -Math.log( 1 - r.nextDouble());
    }
    private double randomForShapeGreaterThan1(){
        double u1, u2, v, y, z, w;
        while (true){
            u1 = r.nextDouble();
            u2 = r.nextDouble();
            v = a*Math.log(u1/(1-u1));
            y = shape * Math.exp(v);
            z = u1*u1*u2;
            w = b+q*v-y;
            if(w+d-4.5*z>=0||w>=Math.log(z))
                return y;
            
        }
    }
    private double randomForShapeLessThan1(){
        double p,y;
        while(true){
            p = r.nextDouble()*b;
            if(p>1){
                y = -Math.log((b-p)/shape);
                if(r.nextDouble()<=Math.pow(y, shape-1))
                    return y;
            }
            y = Math.pow(p, 1/shape);
            if(r.nextDouble()<=Math.exp(-y))
                return y;                
        }
    }
    
    public double getPDF(double x){
        return incompleteGammaFunction().value(x/scale);
    }
    
    private IncompleteGammaFunction incompleteGammaFunction(){
        if(incompleteGammaFunction==null){
            incompleteGammaFunction = new IncompleteGammaFunction(shape);
        }
        return incompleteGammaFunction;
    }
    
    //private number variables
    private double shape;
    private double scale;
    private double norm;
    //private random number variables
    private double a;
    private double b;
    private double q;
    private double d;
    private Random r = new Random();
    private IncompleteGammaFunction incompleteGammaFunction;
    
//    public void resample(int numiter) {
//		int iter, jj, zz;
//	  	double aa, bb, xx, nd;
//	  	for (iter = 0 ;iter < numiter;iter++ ) {
//	    	aa = alphaa;
//	    	bb = alphab;
//	    	for ( jj = 0 ; jj < totalNumData.length ; jj++ ) {
//	    		if (DPs[jj].state != DirichletProcess.HELDOUT) {
//		      		nd = (double) totalNumData[jj];
//		      		if (nd <= 0) {
//		      			xx = 1.0;
//		      		} else {
//		      			xx = Beta.staticNextDouble(alpha+1.0, nd);
//		      		}
//		      		if ( Uniform.staticNextDouble() * (alpha + nd) < nd ) {
//		      			zz = 1;
//		      		}
//		      		else {
//		      			zz = 0;
//		      		}
//		      
//		      		aa += totalNumTables[jj] - zz;
//		      		bb -= Math.log(xx);
//	    		}
//	    	}
//	    	if (aa >= 1.0) {
//	    		alpha = Gamma.staticNextDouble(aa,1.0) / bb;
//	    	}
//	  	}
//	  	updateDPs();
//	}
    
    public static void main(String[] args) throws IOException {
		
    	GammaDistrn gama = new GammaDistrn(5,1);
		
		System.out.println("Inference time: " + gama.getValue());

	}
}
