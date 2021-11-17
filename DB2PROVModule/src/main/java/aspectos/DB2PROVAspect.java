package aspectos;

import java.util.Arrays;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Aspect
public class DB2PROVAspect {

	private Logger log = LoggerFactory.getLogger(getClass());
		// @Around("@annotation(ApplyHelloAspectAdvice)")
//	    public Object hello(ProceedingJoinPoint joinPoint) {
//	        return "Hello from Aspect!";
//	    }
	//	
		 @Pointcut("execution(* io.quantumdb.core.schema.operations.*.*(..))")
//		@Pointcut("execution(public static String *.*StringTest(..))")
		public void atExecution() {
		}

		//@Around("atExecution()")
		@Around("execution(public static String *.*StringTest(..))")
		public Object hello() {
			log.info("**********\n*****Bea*******\n********************");
			log.info("**********\n*****Bea*******\n********************");
			log.info("**********\n*****Bea*******\n********************");
			log.info("**********\n*****Bea*******\n********************");
			log.info("**********\n*****Bea*******\n********************");
			log.info("**********\n*****Bea*******\n********************");
			log.info("**********\n*****Bea*******\n********************");
			return "Hello from Aspect!";
		}

		//@Before("execution(* *.*(..))")
		@Before("atExecution()")
		public void logJoinPoint(JoinPoint joinPoint) {
			System.out.println("**********************************************");
			System.out.println("**********************************************");
			System.out.println("Signature name : " + joinPoint.getSignature().getName());
			log.info("**********\n*****Bea*******\n********************");
			log.info("**********\n*****Bea*******\n********************");
			log.info("**********\n*****Bea*******\n********************");
			
		    
		}
	}
