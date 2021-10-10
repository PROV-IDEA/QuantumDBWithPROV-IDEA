package aspectos;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
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
		// @Pointcut("execution(* *(..))")
//		@Pointcut("execution(public static String *.*StringTest(..))")
//		public void atExecution() {
//		}

		//@Around("atExecution()")
		@Around("execution(public static String *.*StringTest(..))")
		public Object hello(ProceedingJoinPoint joinPoint) {
			log.info("**********\n*****Bea*******\n********************");
			log.info("**********\n*****Bea*******\n********************");
			log.info("**********\n*****Bea*******\n********************");
			log.info("**********\n*****Bea*******\n********************");
			log.info("**********\n*****Bea*******\n********************");
			log.info("**********\n*****Bea*******\n********************");
			log.info("**********\n*****Bea*******\n********************");
			return "Hello from Aspect!";
		}

		@Before("execution(* *.load(..))")
		public void hello2() {
			
			log.info("\n*****************\n*****************\n*****************");
			log.info("\n*****************\n*****************\n*****************");
			log.info("\n*****************\n*****************\n*****************");
			log.info("\n*****************\n*****************\n*****************");
			log.info("\n*****************\n*****************\n*****************");
		}

	}
